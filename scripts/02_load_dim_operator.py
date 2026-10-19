# -*- coding: utf-8 -*-
"""灌 dim_operator（463 行）。

两个数据源：
  1. ../rag_project/operators.json   结构化字段（星级/职业/种族/生日…）
  2. ../rag_project/corpus_db/*.md    属性段里的 攻击间隔/再部署/信赖加成/等级上限

为什么中间要落一个临时 JSON、再从 JSON 读进 Spark：
  Windows 下 createDataFrame 会走 Python worker，在中文路径或某些环境下
  报 "Python worker failed to connect back"。而 spark.read.json 走 JVM 侧，
  不经过 Python worker，最稳。所以：
      Python 拼好数据 → 写成换行分隔的 JSON → spark.read.json → insertInto
  这就是"绕开 Python worker"的具体做法。

运行前必须设 HADOOP_HOME，且脚本要在 ASCII 路径下（项目根目录就行）。
"""
import os
import re
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------- 路径 ----------
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
RAG = os.path.join(ROOT, "..", "rag_project")          # 数据源（只读）
OPERATORS = os.path.join(RAG, "operators.json")
CORPUS = os.path.join(RAG, "corpus_db")
TMP_DIR = os.path.join(ROOT, "_tmp")
TMP_JSON = os.path.join(TMP_DIR, "dim_operator.json")

# ---------- 目标表结构（顺序必须和 DDL 完全一致）----------
# insertInto 是"按位置"匹配列的，不是按列名。
# 所以最后 select 时列的顺序必须和建表语句一模一样，否则数据会错位，
# 而且不会报错——这是最阴的一类 bug。
TARGET = [
    ("operator_name",   "string"),
    ("star",            "int"),
    ("profession",      "string"),
    ("branch",          "string"),
    ("position",        "string"),
    ("race",            "string"),
    ("gender",          "string"),
    ("origin",          "string"),
    ("birthday",        "string"),
    ("height",          "string"),
    ("combat_exp",      "string"),
    ("trait",           "string"),
    ("attack_interval", "string"),
    ("redeploy",        "string"),
    ("trust_hp",        "int"),
    ("trust_atk",       "int"),
    ("trust_def",       "int"),
    ("level_cap_e0",    "int"),
    ("level_cap_e1",    "int"),
    ("level_cap_e2",    "int"),
]


def parse_md(name):
    """从 corpus_db/<name>.md 的属性段里抽 4 类信息。

    属性段长这样：
        ## 属性
        - 再部署 70s  部署费用 19→21→23  阻挡数 3  攻击速度 1.2s
        - 精英0 1级：生命 1602 / 攻击 221 / 防御 257 / 法抗 0
        - 精英0 满级（50级）：生命 2165 / ...
        - 精英1 满级（80级）：生命 2849 / ...
        - 精英2 满级（90级）：生命 3850 / ...
        - 信赖加成：攻击 +60 / 防御 +60
        - 模组：友人们的赠礼（atk:70;def:100;block_cnt:1）；...
    """
    path = os.path.join(CORPUS, name + ".md")
    if not os.path.exists(path):
        return {}

    text = open(path, encoding="utf-8").read()

    # 只取「## 属性」到下一个二级标题之间的内容。
    # 注意：属性段末尾可能在部分文件里残留旧的「所属势力」行（历史遗留），
    # 这里不解析势力，所以不影响。
    i = text.find("## 属性")
    if i < 0:
        return {}
    j = text.find("\n## ", i + 5)
    seg = text[i: j if j > 0 else len(text)]

    out = {}

    # --- 表头行里的两个单值字段 ---
    # 注意字段名叫「攻击速度」，但它存的是【攻击间隔】（秒），越大越慢。
    m = re.search(r"再部署\s*(\S+)", seg)
    out["redeploy"] = m.group(1) if m else None

    m = re.search(r"攻击速度\s*(\S+)", seg)
    out["attack_interval"] = m.group(1) if m else None

    # --- 精英等级上限 ---
    # 3 星及以下没有精英1，4 星以下没有精英2，所以后两个可能是 None。
    # 这里填 None（NULL）而不是 0：这是"该阶段不存在"，不是"上限是 0"。
    for ph in ("0", "1", "2"):
        m = re.search(r"精英" + ph + r" 满级（(\d+)级）", seg)
        out["level_cap_e" + ph] = int(m.group(1)) if m else None

    # --- 信赖加成 ---
    # 源模板里没出现的属性是显式 0，所以"文本里没有"等价于"是 0"，填 0 而不是 NULL。
    trust = {"生命": 0, "攻击": 0, "防御": 0, "法抗": 0}
    m = re.search(r"信赖加成：(.+)", seg)
    if m:
        for k, v in re.findall(r"(生命|攻击|防御|法抗)\s*\+(\d+)", m.group(1)):
            trust[k] = int(v)
    out["trust_hp"] = trust["生命"]
    out["trust_atk"] = trust["攻击"]
    out["trust_def"] = trust["防御"]
    # 法抗从不出现，表里也没这一列，所以不输出

    return out


def main():
    # ---------- 1. 读 operators.json ----------
    ops = json.load(open(OPERATORS, encoding="utf-8"))
    print(f"operators.json: {len(ops)} 条")

    # ---------- 2. 逐条拼装 ----------
    rows, missing_md = [], []
    for o in ops:
        name = o.get("名称")
        if not name:
            continue
        row = {
            "operator_name":   name,
            # 星级在 json 里是 int，但为了保险统一 int()
            "star":            int(o["星级"]) if o.get("星级") not in (None, "") else None,
            "profession":      o.get("职业") or None,   # 空字符串统一成 None，避免库里混着 "" 和 NULL
            "branch":          o.get("分支") or None,
            "position":        o.get("站位") or None,
            "race":            o.get("种族") or None,
            "gender":          o.get("性别") or None,
            "origin":          o.get("出身地") or None,
            "birthday":        o.get("生日") or None,
            "height":          o.get("身高") or None,
            "combat_exp":      o.get("战斗经验") or None,
            "trait":           o.get("特性") or None,
        }
        md = parse_md(name)
        if not md:
            missing_md.append(name)
        row.update(md)
        # 补齐可能缺失的键，避免 JSON 里字段不全导致 Spark 推断出意外结构
        for col, _ in TARGET:
            row.setdefault(col, None)
        rows.append(row)

    print(f"拼装完成: {len(rows)} 行")
    if missing_md:
        print(f"⚠️ 没找到属性文件的: {len(missing_md)} 个 -> {missing_md[:5]}")

    # ---------- 3. 落成换行分隔的 JSON ----------
    os.makedirs(TMP_DIR, exist_ok=True)
    with open(TMP_JSON, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"临时文件: {TMP_JSON}")

    # ---------- 4. Spark 读进来并写表 ----------
    spark = (SparkSession.builder
             .appName("load_dim_operator")
             .master("local[2]")
             .config("spark.sql.warehouse.dir",
                     "file:///" + os.path.join(ROOT, "spark-warehouse").replace("\\", "/"))
             .config("spark.driver.memory", "2g")
             .enableHiveSupport()
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql("USE arknights_dw")

    df = spark.read.json(TMP_JSON)

    # 显式 cast + 按目标顺序 select。
    # 不 cast 的话，JSON 里的数字会被读成 bigint，写进 INT 列可能报类型不匹配；
    # 不排序的话，insertInto 会按位置错位。
    df2 = df.select([F.col(c).cast(t).alias(c) for c, t in TARGET])

    # mode("overwrite") + insertInto = INSERT OVERWRITE，会先清空再写，
    # 所以这个脚本可以反复跑，不会重复插入。
    df2.write.mode("overwrite").insertInto("dim_operator")

    # ---------- 5. 校验 ----------
    print("\n=== 校验 ===")
    print("行数:", spark.table("dim_operator").count())

    # 注意：Spark SQL 的列别名如果是非 ASCII（中文），必须用反引号包起来，
    # 否则报 INVALID_IDENTIFIER: The unquoted identifier 总数 is invalid。
    # 这是 Spark 4 的标识符校验变严了，MySQL 里不会报这个错。
    spark.sql("""
      SELECT
        COUNT(*)                                                        AS `总数`,
        SUM(CASE WHEN star IS NULL THEN 1 ELSE 0 END)                   AS `星级空`,
        SUM(CASE WHEN race IS NULL THEN 1 ELSE 0 END)                   AS `种族空`,
        SUM(CASE WHEN attack_interval IS NULL THEN 1 ELSE 0 END)        AS `攻击间隔空`,
        SUM(CASE WHEN redeploy IS NULL THEN 1 ELSE 0 END)               AS `再部署空`,
        SUM(CASE WHEN trust_atk > 0 THEN 1 ELSE 0 END)                  AS `有攻击信赖`,
        SUM(CASE WHEN level_cap_e2 IS NOT NULL THEN 1 ELSE 0 END)       AS `有精英2上限`,
        SUM(CASE WHEN level_cap_e1 IS NULL THEN 1 ELSE 0 END)           AS `无精英1上限`
      FROM dim_operator
    """).show(truncate=False)

    spark.sql("SELECT operator_name,star,profession,attack_interval,redeploy,trust_hp,trust_atk,trust_def,level_cap_e0,level_cap_e1,level_cap_e2 FROM dim_operator WHERE operator_name IN ('凯尔希','星熊','芬','巡林者','百炼嘉维尔') ORDER BY operator_name").show(truncate=False)

    # 抽查信頼加成最多的那类（攻击）
    spark.sql("SELECT operator_name, trust_atk FROM dim_operator ORDER BY trust_atk DESC LIMIT 5").show()

    spark.stop()


if __name__ == "__main__":
    main()
