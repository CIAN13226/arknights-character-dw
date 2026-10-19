# -*- coding: utf-8 -*-
"""灌 fact_operator_attr（1,796 行）—— 属性事实表。

粒度：一个干员 × 一个精英阶段档位。

从 corpus_db/<name>.md 的属性段解析：

    - 再部署 70s  部署费用 19→21→23  阻挡数 3  攻击速度 1.2s
    - 精英0 1级：生命 1602 / 攻击 221 / 防御 257 / 法抗 0
    - 精英0 满级（50级）：生命 2165 / 攻击 284 / 防御 384 / 法抗 0
    - 精英1 满级（80级）：生命 2849 / 攻击 356 / 防御 527 / 法抗 0
    - 精英2 满级（90级）：生命 3850 / 攻击 430 / 防御 723 / 法抗 0

phase_id 与阶段的对应（跟 dim_phase 表一致）：
    1 = 精英0 1级
    2 = 精英0 满级
    3 = 精英1 满级
    4 = 精英2 满级

两条链式字段的映射规则（实测全库的段数分布后定的）：

    阻挡数：
        段数恒等于精英阶段数（>1 时），所以直接 1:1 映射。
        段数为 1 时全阶段同值。

    部署费用：
        段数有 1/2/3 三种，且**不等于**阶段数：
          1 段 -> 全阶段同值（17 个干员，2 星及以下）
          段数 == 阶段数 -> 直接 1:1（3 星的 2 段、6 星的 3 段）
          阶段 3 但只有 2 段 -> **假设 E1 = E2 = 第二个值**（321 个干员）
            依据：芬是 3 星只有 E0/E1，其 "9→11" 的第二个值只能是 E1，
                  说明链的第二个值是"涨价后的值"。6 星只涨一次时，
                  涨价发生在 E1，E2 延续 E1 的值。
            这条是**假设**，源数据只给了端点。已在 README 的已知限制里写明。

跑完预期：
    总行数 1,796
    phase 1 / 2 各 463 行（所有干员都有精英0）
    phase 3 446 行（463 - 17 个没有精英1）
    phase 4 424 行
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
TMP_JSON = os.path.join(TMP_DIR, "fact_operator_attr.json")

SCHEMA = [("operator_name", "string"), ("phase_id", "int"),
          ("hp", "int"), ("atk", "int"), ("def", "int"), ("res", "int"),
          ("block", "int"), ("cost", "int")]

# 匹配 "- 精英0 1级：..." 和 "- 精英1 满级（80级）：..."
PHASE_RE = re.compile(r"^-\s*精英(\d)\s*(1级|满级)[^：]*：(.*)$")

# (精英等级, 档位) -> phase_id
PHASE_ID = {(0, "1级"): 1, (0, "满级"): 2, (1, "满级"): 3, (2, "满级"): 4}


def parse_stats(text):
    """'生命 1602 / 攻击 221 / 防御 257 / 法抗 0' -> dict"""
    out = {}
    for key, col in (("生命", "hp"), ("攻击", "atk"), ("防御", "def"), ("法抗", "res")):
        m = re.search(key + r"\s*(\d+)", text)
        out[col] = int(m.group(1)) if m else None
    return out


def expand_chain(raw, n_elite, field):
    """把 '19→21→23' 这种链展开成 {0:.., 1:.., 2:..}（按精英等级索引）。

    n_elite = 该干员有几个精英阶段（1/2/3）
    field   = 'block' 或 'cost'，只用于对不同的假设策略做区分
    """
    if not raw:
        return {}
    parts = [p.strip() for p in raw.replace("，", ",").split("→") if p.strip()]
    if not parts:
        return {}

    # 1 段：全阶段同值
    if len(parts) == 1:
        return {0: parts[0], 1: parts[0], 2: parts[0]}

    # 段数 == 阶段数：直接对应
    if len(parts) == n_elite:
        return {i: parts[i] for i in range(len(parts))}

    # 费用特有：3 个阶段但只给了 2 段 -> E0=第一个，E1=E2=第二个（假设）
    if field == "cost" and n_elite == 3 and len(parts) == 2:
        return {0: parts[0], 1: parts[1], 2: parts[1]}

    # 兜底：不够就用最后一个值补齐（正常数据走不到这里，会打警告）
    return {i: parts[min(i, len(parts) - 1)] for i in range(3)}


def to_int(v):
    try:
        return int(str(v).strip())
    except (TypeError, ValueError):
        return None


def main():
    rows = []
    no_attr, odd = [], []

    for fn in os.listdir(CORPUS):
        if not fn.endswith(".md"):
            continue
        name = fn[:-3]
        text = open(os.path.join(CORPUS, fn), encoding="utf-8").read()

        i = text.find("## 属性")
        if i < 0:
            no_attr.append(name)
            continue
        j = text.find("\n## ", i + 5)
        seg = text[i: j if j > 0 else len(text)]

        # --- 找出这个干员有几个精英阶段 ---
        phases = []          # [(phase_id, {hp,atk,def,res}), ...]
        max_elite = 0
        for line in seg.split("\n"):
            m = PHASE_RE.match(line.strip())
            if not m:
                continue
            elite, grade = int(m.group(1)), m.group(2)
            pid = PHASE_ID.get((elite, grade))
            if pid is None:
                continue
            phases.append((pid, parse_stats(m.group(3)), elite))
            max_elite = max(max_elite, elite)

        n_elite = max_elite + 1        # 精英阶段数：1/2/3

        # --- 两条链式字段 ---
        mb = re.search(r"阻挡数\s*(\S+)", seg)
        mc = re.search(r"部署费用\s*(\S+)", seg)
        block_map = expand_chain(mb.group(1) if mb else None, n_elite, "block")
        cost_map = expand_chain(mc.group(1) if mc else None, n_elite, "cost")

        # 记录异常（链段数和阶段数对不上、且不是已知的费用情形）
        for fld, raw, mp in (("block", mb.group(1) if mb else None, block_map),
                             ("cost", mc.group(1) if mc else None, cost_map)):
            if raw and "→" in raw:
                nseg = raw.count("→") + 1
                if nseg != n_elite and not (fld == "cost" and n_elite == 3 and nseg == 2):
                    odd.append((name, fld, raw, n_elite))

        # --- 组装事实行 ---
        for pid, st, elite in phases:
            rows.append({
                "operator_name": name,
                "phase_id": pid,
                "hp": st.get("hp"),
                "atk": st.get("atk"),
                "def": st.get("def"),
                "res": st.get("res"),
                "block": to_int(block_map.get(elite)),
                "cost": to_int(cost_map.get(elite)),
            })

    print(f"拼装 {len(rows)} 行")
    if no_attr:
        print(f"⚠️ 没有属性段的干员: {len(no_attr)} 个 -> {no_attr[:5]}")
    if odd:
        print(f"⚠️ 链段数与阶段数不符且不在已知情形内: {len(odd)} 条")
        for n, f, r, ne in odd[:8]:
            print(f"   {n:<14} {f} raw={r} 阶段数={ne}")

    # --- 落临时 JSON ---
    os.makedirs(TMP_DIR, exist_ok=True)
    with open(TMP_JSON, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    # --- Spark 写入 ---
    spark = (SparkSession.builder
             .appName("load_fact_attr")
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
      .write.mode("overwrite").insertInto("fact_operator_attr")
    print(f"写入 fact_operator_attr: {spark.table('fact_operator_attr').count()} 行")

    # --- 校验 ---
    print("\n=== 校验 ===")
    # 中文别名要加反引号，引用处（ORDER BY / GROUP BY 结果名）也要加
    spark.sql("""
      SELECT phase_id AS `阶段`, COUNT(*) AS `行数`
      FROM fact_operator_attr GROUP BY phase_id ORDER BY phase_id
    """).show(truncate=False)

    spark.sql("""
      SELECT
        COUNT(*)                                              AS `总行数`,
        SUM(CASE WHEN hp IS NULL THEN 1 ELSE 0 END)           AS `hp空`,
        SUM(CASE WHEN block IS NULL THEN 1 ELSE 0 END)        AS `阻挡空`,
        SUM(CASE WHEN cost IS NULL THEN 1 ELSE 0 END)         AS `费用空`,
        SUM(CASE WHEN hp = 0 THEN 1 ELSE 0 END)               AS `hp为0`,
        SUM(CASE WHEN atk = 0 THEN 1 ELSE 0 END)              AS `atk为0`
      FROM fact_operator_attr
    """).show(truncate=False)

    print("抽查（看着三个阶段的数据对不对）：")
    spark.sql("""
      SELECT operator_name AS `干员`, phase_id AS `阶段`, hp AS `生命`, atk AS `攻击`,
             def AS `防御`, res AS `法抗`, block AS `阻挡`, cost AS `费用`
      FROM fact_operator_attr
      WHERE operator_name IN ('凯尔希','星熊','巡林者','芬')
      ORDER BY operator_name, phase_id
    """).show(truncate=False)

    print("阻挡数随阶段变的例子（百炼嘉维尔应该是 2/2/2/3）：")
    spark.sql("""
      SELECT operator_name AS `干员`, phase_id AS `阶段`, block AS `阻挡`, cost AS `费用`
      FROM fact_operator_attr WHERE operator_name = '百炼嘉维尔' ORDER BY phase_id
    """).show(truncate=False)

    # 第一条真正的业务查询：每个职业的平均攻击力（精英2 满级）
    print("每个职业在精英2满级时的平均攻击力：")
    spark.sql("""
      SELECT o.profession AS `职业`,
             COUNT(*)         AS `人数`,
             ROUND(AVG(f.atk), 1) AS `平均攻击`,
             MAX(f.atk)       AS `最高攻击`
      FROM fact_operator_attr f
      JOIN dim_operator o ON f.operator_name = o.operator_name
      WHERE f.phase_id = 4
      GROUP BY o.profession
      ORDER BY `平均攻击` DESC
    """).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
