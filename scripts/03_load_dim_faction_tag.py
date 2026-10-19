# -*- coding: utf-8 -*-
"""灌 dim_faction（多值维度）和 dim_tag（多值维度）。

两个表都只依赖 operators.json，不用解析 markdown，是最简单的两张。

数据形态：
    operators.json 里
      "势力层级":     ["维多利亚", "塔拉"]        ← 所属势力，逗号分隔=层级
      "隐藏势力列表":  ["炎", "维多利亚"]           ← 隐藏势力，一般 ;;/; 分隔=并列
      "标签":         "召唤 治疗"                  ← 空格分隔

    一行 dim_faction = 一个干员的一个势力归属（relation 区分所属/隐藏）
    一行 dim_tag     = 一个干员的一个标签

已知脏数据（上游 bug，这里防御性处理 + 打警告）：
    有 7 个干员的「隐藏势力」用了逗号而不是分号，导致 2 个势力被当成 1 个：
      初雪/崖心/极光/耶拉: 谢拉格,喀兰贸易
      维娜·维多利亚:      维多利亚,格拉斯哥帮
      苇草:               维多利亚,塔拉
      莫斯提马:           炎-龙门,企鹅物流
    根因在 fetch_prts.py 的 parse_faction()：隐藏势力只按 ;;/; 切，没按逗号切。
    这里的做法是"加载时拆开 + 告警"，保证表里数据正确，同时不掩盖上游问题。

隐藏势力的 faction_level 一律填 NULL：
    隐藏势力的原始写法有两种（;; 并列 / 逗号层级），拆开后已经分不清层级，
    与其猜一个值，不如留空。查"谁算某势力"只用 faction_name，不受影响。
"""
import os
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
RAG = os.path.join(ROOT, "..", "rag_project")
OPERATORS = os.path.join(RAG, "operators.json")
TMP_DIR = os.path.join(ROOT, "_tmp")

FACTION_JSON = os.path.join(TMP_DIR, "dim_faction.json")
TAG_JSON = os.path.join(TMP_DIR, "dim_tag.json")

FACTION_SCHEMA = [("operator_name", "string"), ("faction_name", "string"),
                  ("relation", "string"), ("faction_level", "int")]
TAG_SCHEMA = [("operator_name", "string"), ("tag", "string")]


def split_multi(v):
    """把 '谢拉格,喀兰贸易' 拆成 ['谢拉格','喀兰贸易']；已经是 list 就先摊平再拆。"""
    if not v:
        return []
    if isinstance(v, list):
        items = v
    else:
        items = [v]
    out = []
    for it in items:
        for part in str(it).replace("，", ",").split(","):
            part = part.strip()
            if part:
                out.append(part)
    return out


def main():
    ops = json.load(open(OPERATORS, encoding="utf-8"))
    print(f"operators.json: {len(ops)} 条")

    faction_rows, tag_rows = [], []
    dirty = []          # 记录被加载器"救回来"的脏数据

    for o in ops:
        name = o.get("名称")
        if not name:
            continue

        # ---- 所属势力：层级有顺序，faction_level 用位置（1,2）----
        owned = [x for x in o.get("势力层级", []) if x]
        for idx, fname in enumerate(owned, start=1):
            faction_rows.append({
                "operator_name": name,
                "faction_name": fname,
                "relation": "所属",
                "faction_level": idx,
            })

        # ---- 隐藏势力：可能带逗号（脏数据），这里拆开并告警 ----
        hidden_raw = o.get("隐藏势力列表", []) or []
        hidden_split = split_multi(hidden_raw)
        if len(hidden_split) != len([x for x in hidden_raw if x]):
            dirty.append((name, hidden_raw, hidden_split))
        for fname in hidden_split:
            faction_rows.append({
                "operator_name": name,
                "faction_name": fname,
                "relation": "隐藏",
                "faction_level": None,      # 拆开后层级信息不可靠，留空
            })

        # ---- 标签：空格分隔 ----
        for tag in (o.get("标签") or "").split():
            tag_rows.append({"operator_name": name, "tag": tag})

    print(f"dim_faction 拼装: {len(faction_rows)} 行")
    print(f"dim_tag     拼装: {len(tag_rows)} 行")

    if dirty:
        print(f"\n⚠️ 有 {len(dirty)} 条隐藏势力被加载器拆开（上游 bug，建议修 parse_faction）：")
        for n, before, after in dirty:
            print(f"   {n:<14} {before} -> {after}")

    # ---- 落临时 JSON ----
    os.makedirs(TMP_DIR, exist_ok=True)
    for path, rows in ((FACTION_JSON, faction_rows), (TAG_JSON, tag_rows)):
        with open(path, "w", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"\n临时文件已写: {TMP_DIR}")

    # ---- Spark 写入 ----
    spark = (SparkSession.builder
             .appName("load_faction_tag")
             .master("local[2]")
             .config("spark.sql.warehouse.dir",
                     "file:///" + os.path.join(ROOT, "spark-warehouse").replace("\\", "/"))
             .config("spark.driver.memory", "2g")
             .enableHiveSupport()
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql("USE arknights_dw")

    for table, path, schema in (("dim_faction", FACTION_JSON, FACTION_SCHEMA),
                                ("dim_tag", TAG_JSON, TAG_SCHEMA)):
        df = spark.read.json(path)
        df.select([F.col(c).cast(t).alias(c) for c, t in schema]) \
          .write.mode("overwrite").insertInto(table)
        print(f"写入 {table}: {spark.table(table).count()} 行")

    # ---- 校验 ----
    print("\n=== 校验 ===")
    spark.sql("""
      SELECT relation AS `类别`, COUNT(*) AS `行数`, COUNT(DISTINCT operator_name) AS `干员数`
      FROM dim_faction GROUP BY relation
    """).show(truncate=False)

    print("按一级势力统计人数（前 8）：")
    # 注意：中文别名加反引号，而且**引用它的地方（ORDER BY）也要加**，不只是定义处。
    # 只给 AS 那侧加、ORDER BY 不加，同样报 INVALID_IDENTIFIER。
    spark.sql("""
      SELECT faction_name AS `势力`, COUNT(*) AS `人数`
      FROM dim_faction
      WHERE relation = '所属' AND faction_level = 1
      GROUP BY faction_name ORDER BY `人数` DESC LIMIT 8
    """).show(truncate=False)

    print("隐藏势力 = 叙拉古 的干员（应该能查到予愿安洁莉娜）：")
    spark.sql("""
      SELECT operator_name AS `干员` FROM dim_faction
      WHERE relation = '隐藏' AND faction_name = '叙拉古'
    """).show(truncate=False)

    print("标签出现次数 Top 8：")
    spark.sql("""
      SELECT tag AS `标签`, COUNT(*) AS `人数`
      FROM dim_tag GROUP BY tag ORDER BY `人数` DESC LIMIT 8
    """).show(truncate=False)

    print("那个被拆开的脏数据（维娜·维多利亚 应该有 2 行隐藏势力）：")
    spark.sql("""
      SELECT operator_name AS `干员`, faction_name AS `势力`, relation AS `类别`
      FROM dim_faction WHERE operator_name = '维娜·维多利亚'
    """).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
