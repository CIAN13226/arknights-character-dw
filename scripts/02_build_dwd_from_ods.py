# -*- coding: utf-8 -*-
"""从 ODS 重建全部 6 张 DWD 表（dim_operator / dim_faction / dim_tag /
dim_module / dim_potential / fact_operator_attr）。

为什么合并成一个脚本，而不是改原来的 5 个：
    原来的 02~06 各自读源文件（operators.json + corpus_db），互不相干。
    现在数据源统一是 ODS 表，如果还分 5 个脚本，
    就要把 463 份 wikitext 从 Spark 拉 5 次、解析 5 次，纯浪费。
    合并后：拉一次 → 解析一次 → 同时产出 6 张表。

分层依赖（这次改造的核心）：
    ODS(ods_prts_wikitext) -> 本脚本 -> DWD(6 张表)
    本脚本**只读 ods_prts_wikitext**，不碰 operators.json、不碰 corpus_db。
    这是"每层只读上一层"的落地。

dim_phase 不在这里重建：它是 4 行静态数据，和 wikitext 无关。

校验基准（改造后必须完全一致，否则说明解析退化了）：
    dim_operator        463
    dim_faction         668   (所属 545 + 隐藏 123)
    dim_tag             819
    dim_module          513
    dim_potential       2155
    fact_operator_attr  1796  (阶段分布 463/463/446/424)
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)

import dw_parse                    # noqa: E402

# ---- 各表列顺序必须和 DDL 一致（insertInto 按位置匹配）----
OPERATOR_COLS = [
    ("operator_name", "string"), ("star", "int"), ("profession", "string"),
    ("branch", "string"), ("position", "string"), ("race", "string"),
    ("gender", "string"), ("origin", "string"), ("birthday", "string"),
    ("height", "string"), ("combat_exp", "string"), ("trait", "string"),
    ("attack_interval", "string"), ("redeploy", "string"),
    ("trust_hp", "int"), ("trust_atk", "int"), ("trust_def", "int"),
    ("level_cap_e0", "int"), ("level_cap_e1", "int"), ("level_cap_e2", "int"),
]
FACTION_COLS = [("operator_name", "string"), ("faction_name", "string"),
                ("relation", "string"), ("faction_level", "int")]
TAG_COLS = [("operator_name", "string"), ("tag", "string")]
MODULE_COLS = [
    ("operator_name", "string"), ("module_name", "string"),
    ("bonus_atk", "int"), ("bonus_def", "int"), ("bonus_hp", "int"),
    ("bonus_res", "int"), ("bonus_block", "int"), ("bonus_attack_speed", "int"),
    ("bonus_cost", "int"), ("bonus_respawn_time", "int"),
]
POTENTIAL_COLS = [("operator_name", "string"), ("potential_level", "int"),
                  ("bonus_type", "string"), ("bonus_value", "int"),
                  ("is_special", "boolean")]
FACT_COLS = [("operator_name", "string"), ("phase_id", "int"),
             ("hp", "int"), ("atk", "int"), ("def", "int"), ("res", "int"),
             ("block", "int"), ("cost", "int")]

EXPECT = {
    "dim_operator": 463,
    "dim_faction": 668,
    "dim_tag": 819,
    "dim_module": 513,
    "dim_potential": 2155,
    "fact_operator_attr": 1796,
}


def main():
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = (SparkSession.builder
             .appName("build_dwd_from_ods")
             .master("local[2]")
             .config("spark.sql.warehouse.dir",
                     "file:///" + os.path.join(ROOT, "spark-warehouse").replace("\\", "/"))
             .config("spark.driver.memory", "2g")
             .enableHiveSupport()
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql("USE arknights_dw")

    # ---- 1. 只从 ODS 读 ----
    ods = spark.sql("SELECT operator_name, wikitext FROM ods_prts_wikitext").collect()
    print(f"从 ODS 读到 {len(ods)} 条 wikitext", flush=True)

    # ---- 2. 一次遍历，产出 6 张表的行 ----
    op_rows, fac_rows, tag_rows = [], [], []
    mod_rows, pot_rows, fact_rows = [], [], []
    no_attr, bad = [], []

    for k, r in enumerate(ods, 1):
        name, wt = r["operator_name"], r["wikitext"]

        # dim_operator
        o = dw_parse.parse_operator(wt)
        row = {"operator_name": name}
        for col, _ in OPERATOR_COLS:
            if col != "operator_name":
                row[col] = o.get(col)
        op_rows.append(row)

        # dim_tag
        for t in dw_parse.parse_tags(wt):
            tag_rows.append({"operator_name": name, "tag": t})

        # dim_faction
        for fname, rel, lvl in dw_parse.parse_factions(wt):
            fac_rows.append({"operator_name": name, "faction_name": fname,
                             "relation": rel, "faction_level": lvl})

        # dim_module
        for mname, cols in dw_parse.parse_modules(wt):
            mrow = {"operator_name": name, "module_name": mname}
            for col, _ in MODULE_COLS:
                if col not in ("operator_name", "module_name"):
                    mrow[col] = cols.get(col)
            mod_rows.append(mrow)

        # dim_potential
        for lvl, btype, bval, special in dw_parse.parse_potentials(wt):
            pot_rows.append({"operator_name": name, "potential_level": lvl,
                             "bonus_type": btype, "bonus_value": bval,
                             "is_special": special})

        # fact_operator_attr
        attrs = dw_parse.parse_attributes(wt)
        if not attrs:
            no_attr.append(name)
        for a in attrs:
            frow = {"operator_name": name}
            frow.update(a)
            fact_rows.append(frow)

        if k % 100 == 0:
            print(f"  解析 {k}/{len(ods)}", flush=True)

    print(f"\n解析完成：", flush=True)
    print(f"   dim_operator        {len(op_rows)}", flush=True)
    print(f"   dim_faction         {len(fac_rows)}", flush=True)
    print(f"   dim_tag             {len(tag_rows)}", flush=True)
    print(f"   dim_module          {len(mod_rows)}", flush=True)
    print(f"   dim_potential       {len(pot_rows)}", flush=True)
    print(f"   fact_operator_attr  {len(fact_rows)}", flush=True)
    if no_attr:
        print(f"   ⚠️ 没有解析出属性行的干员 {len(no_attr)} 个: {no_attr[:8]}", flush=True)

    # ---- 3. 写临时 JSON，再进 Spark（避开 Python worker）----
    import json
    tmp = os.path.join(ROOT, "_tmp")
    os.makedirs(tmp, exist_ok=True)

    def write_jsonl(path, rows):
        with open(path, "w", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
        return path

    jobs = [
        ("dim_operator",        write_jsonl(os.path.join(tmp, "dw_op.json"),  op_rows),   OPERATOR_COLS),
        ("dim_faction",         write_jsonl(os.path.join(tmp, "dw_fac.json"), fac_rows),  FACTION_COLS),
        ("dim_tag",             write_jsonl(os.path.join(tmp, "dw_tag.json"), tag_rows),  TAG_COLS),
        ("dim_module",          write_jsonl(os.path.join(tmp, "dw_mod.json"), mod_rows),  MODULE_COLS),
        ("dim_potential",       write_jsonl(os.path.join(tmp, "dw_pot.json"), pot_rows),  POTENTIAL_COLS),
        ("fact_operator_attr",  write_jsonl(os.path.join(tmp, "dw_fact.json"), fact_rows), FACT_COLS),
    ]

    print("\n写入 DWD 各表：", flush=True)
    for table, path, cols in jobs:
        df = spark.read.json(path)
        df.select([F.col(c).cast(t).alias(c) for c, t in cols]) \
          .write.mode("overwrite").insertInto(table)
        n = spark.table(table).count()
        exp = EXPECT.get(table)
        flag = "✅" if exp is None or n == exp else f"❌ 期望 {exp}"
        print(f"   {table:<20} {n:>6} 行  {flag}", flush=True)

    # ---- 4. 和基准对账 ----
    print("\n=== 改造后一致性校验 ===", flush=True)
    all_ok = True
    for tb, exp in EXPECT.items():
        n = spark.table(tb).count()
        if n != exp:
            all_ok = False
            print(f"   ❌ {tb}: {n} != {exp}", flush=True)
    print(("   ✅ 6 张表行数与改造前完全一致" if all_ok else "   ⚠️ 有差异，见上"), flush=True)

    # ---- 5. 数值抽样对比（和之前记录的基准值比）----
    print("\n=== 抽样校验（凯尔希：cost 18/18/20/20，hp 865/1219/1469/1633）===", flush=True)
    spark.sql("""
      SELECT operator_name AS `干员`, phase_id AS `阶段`, hp AS `生命`,
             atk AS `攻击`, block AS `阻挡`, cost AS `费用`
      FROM fact_operator_attr WHERE operator_name IN ('凯尔希','星熊','巡林者')
      ORDER BY operator_name, phase_id
    """).show(truncate=False)

    print("=== 星熊潜能（应为 cost-1 / 天赋类 / def+30 / cost-1 / 天赋类）===", flush=True)
    spark.sql("""
      SELECT operator_name AS `干员`, potential_level AS `潜能`,
             bonus_type AS `类型`, bonus_value AS `数值`, is_special AS `天赋类`
      FROM dim_potential WHERE operator_name='星熊' ORDER BY potential_level
    """).show(truncate=False)

    print("=== 一级势力人数 Top5（应仍是 罗德岛94 哥伦比亚49 维多利亚39）===", flush=True)
    spark.sql("""
      SELECT faction_name AS `势力`, COUNT(*) AS `人数`
      FROM dim_faction WHERE relation='所属' AND faction_level=1
      GROUP BY faction_name ORDER BY `人数` DESC LIMIT 5
    """).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
