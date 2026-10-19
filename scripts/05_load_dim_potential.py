# -*- coding: utf-8 -*-
"""灌 dim_potential（2,155 行）。

数据源：corpus_db/<name>.md 的「## 潜能」段，格式已经是中文可读文本：

    - 潜能2：部署费用-1
    - 潜能3：第一天赋效果增强
    - 潜能4：防御力+30
    - 潜能5：部署费用-1
    - 潜能6：第二天赋效果增强

所以不用解析原始的双数组（潜能 / 潜能类型），直接按句式匹配。

全库只有 12 种句式（已扫过）： 次数
    部署费用-N              902
    再部署时间-N秒           418
    攻击力+N                267
    天赋效果增强             183
    第二天赋效果增强         109
    第一天赋效果增强          98
    天赋效果加强              60   ← 注意是"加强"不是"增强"，别漏
    防御力+N                 43
    生命上限+N               39
    攻击速度+N               23
    法术抗性+N               12
    最大生命值+N               1   ← 只有 1 条，写法和其他不同

数值 = 去掉符号后的数字乘符号（部署费用/再部署时间在文本里自带负号）。
天赋类效果没有数值，用 is_special = true 标记。

行数构成：431 个干员 × 5 个潜能（潜能2~潜能6）= 2,155。
另有 32 个干员（卫戍协议的特殊干员等）潜能段是「- （无）」，不产出行。
"""
import os
import re
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
RAG = os.path.join(ROOT, "..", "rag_project")
CORPUS = os.path.join(RAG, "corpus_db")
TMP_DIR = os.path.join(ROOT, "_tmp")
TMP_JSON = os.path.join(TMP_DIR, "dim_potential.json")

SCHEMA = [("operator_name", "string"), ("potential_level", "int"),
          ("bonus_type", "string"), ("bonus_value", "int"), ("is_special", "boolean")]

LINE_RE = re.compile(r"^-\s*潜能(\d)：(.+)$")

# 描述前缀 -> bonus_type
TYPE_MAP = [
    ("部署费用",   "cost"),
    ("再部署时间", "re_deploy"),
    ("攻击力",     "atk"),
    ("防御力",     "def"),
    ("生命上限",   "hp"),
    ("最大生命值", "hp"),          # 只有凛御银灰 1 条，但写法不同，必须单列
    ("攻击速度",   "attack_speed"),
    ("法术抗性",   "res"),
]

# 天赋类：没有数值
SPECIAL_KEYWORDS = ("天赋效果增强", "天赋效果加强", "第一天赋效果增强", "第二天赋效果增强")

NUM_RE = re.compile(r"([+-])(\d+)")


def parse_desc(desc):
    """'防御力+30' -> ('def', 30, False)
       '第一天赋效果增强' -> (None, None, True)"""
    if any(k in desc for k in SPECIAL_KEYWORDS):
        return None, None, True

    for prefix, btype in TYPE_MAP:
        if desc.startswith(prefix):
            m = NUM_RE.search(desc)
            if not m:
                return btype, None, False
            sign, num = m.group(1), int(m.group(2))
            return btype, (num if sign == "+" else -num), False

    # 没匹配上的：不猜，交给外面打警告
    return None, None, False


def main():
    rows = []
    unknown, levels = [], set()
    no_section = []

    for fn in os.listdir(CORPUS):
        if not fn.endswith(".md"):
            continue
        name = fn[:-3]
        text = open(os.path.join(CORPUS, fn), encoding="utf-8").read()

        i = text.find("## 潜能")
        if i < 0:
            no_section.append(name)
            continue
        j = text.find("\n## ", i + 5)
        seg = text[i: j if j > 0 else len(text)]

        for line in seg.split("\n"):
            m = LINE_RE.match(line.strip())
            if not m:
                continue
            lvl = int(m.group(1))
            desc = m.group(2).strip()
            levels.add(lvl)

            btype, bval, special = parse_desc(desc)
            if btype is None and not special:
                unknown.append((name, desc))
            rows.append({
                "operator_name": name,
                "potential_level": lvl,
                "bonus_type": btype,
                "bonus_value": bval,
                "is_special": special,
            })

    print(f"拼装 {len(rows)} 行")
    print(f"潜能等级取值: {sorted(levels)}")
    if no_section:
        print(f"⚠️ 没有潜能段的干员: {len(no_section)}")
    if unknown:
        print(f"⚠️ 没识别出的描述 {len(unknown)} 条：")
        for n, d in unknown[:10]:
            print(f"   {n:<14} {d}")

    os.makedirs(TMP_DIR, exist_ok=True)
    with open(TMP_JSON, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    spark = (SparkSession.builder
             .appName("load_dim_potential")
             .master("local[2]")
             .config("spark.sql.warehouse.dir",
                     "file:///" + os.path.join(ROOT, "spark-warehouse").replace("\\", "/"))
             .config("spark.driver.memory", "2g")
             .enableHiveSupport()
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql("USE arknights_dw")

    df = spark.read.json(TMP_JSON)
    df.select([F.col(c).cast(t).alias(c) for c, t in SCHEMA]) \
      .write.mode("overwrite").insertInto("dim_potential")
    print(f"写入 dim_potential: {spark.table('dim_potential').count()} 行")

    print("\n=== 校验 ===")
    spark.sql("""
      SELECT bonus_type AS `加成类型`, COUNT(*) AS `条数`,
             SUM(bonus_value) AS `合计数值`
      FROM dim_potential GROUP BY bonus_type ORDER BY `条数` DESC
    """).show(truncate=False)

    spark.sql("""
      SELECT
        COUNT(*)                                                    AS `总行数`,
        COUNT(DISTINCT operator_name)                               AS `干员数`,
        SUM(CASE WHEN is_special THEN 1 ELSE 0 END)                 AS `天赋类`,
        SUM(CASE WHEN bonus_value IS NULL AND NOT is_special THEN 1 ELSE 0 END) AS `空值异常`
      FROM dim_potential
    """).show(truncate=False)

    print("星熊的 5 条潜能（对照页面：费用-1 / 第一天赋增强 / 防御+30 / 费用-1 / 第二天赋增强）：")
    spark.sql("""
      SELECT operator_name AS `干员`, potential_level AS `潜能`, bonus_type AS `类型`,
             bonus_value AS `数值`, is_special AS `天赋类`
      FROM dim_potential WHERE operator_name = '星熊' ORDER BY potential_level
    """).show(truncate=False)

    print("攻击速度类潜能（巡林者应该是 潜能3/4 各 +6）：")
    spark.sql("""
      SELECT operator_name AS `干员`, potential_level AS `潜能`, bonus_value AS `数值`
      FROM dim_potential WHERE operator_name = '巡林者' ORDER BY potential_level
    """).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
