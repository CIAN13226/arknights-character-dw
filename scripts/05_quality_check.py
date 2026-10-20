# -*- coding: utf-8 -*-
"""数据质量校验：把前面各层"零散做过"的检查固化成脚本。

为什么要单独写这个脚本，而不是散在各构建脚本里：
    构建脚本里的校验是"当时那一批数据"的快照。
    数据重跑、上游变了、有人手工改了一张表——这些散落的校验发现不了。
    这个脚本是**可重复执行的全量体检**，任何时候跑一遍就知道库还是不是好的。

检查五类：
    1. 行数基线      —— 各表行数必须等于已知基线，整个都不许变
    2. 主键唯一性    —— 每个表的主键组合不许重复（没有约束时只能靠查）
    3. 外键完整性    —— 事实表里的 operator_name / phase_id 必须能在维度表里找到
    4. 跨层一致性    —— ODS 的干员集合必须和 DWD 的干员集合完全相同
    5. 取值合理性    —— 星级在 1-6、属性为正、关键字段不为空

输出：逐项 PASS/FAIL + 汇总；有不通过则进程退出码 1（便于挂到调度里当质量门禁）。

用法：
    $env:HADOOP_HOME = "E:\hadoop"; $env:PATH = "E:\hadoop\bin;$env:PATH"
    python scripts\\05_quality_check.py
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)

# ---- 行数基线（改造/重跑后必须一致）----
ROW_BASELINE = [
    ("ods_prts_wikitext",    463),
    ("dim_operator",         463),
    ("dim_phase",              4),
    ("dim_faction",          668),
    ("dim_tag",              819),
    ("dim_module",           513),
    ("dim_potential",       2155),
    ("fact_operator_attr",  1796),
    ("dws_faction_star",     283),
    ("dws_profession_branch", 288),
    ("dws_tag",               18),
    ("dws_module_effect",      8),
    ("dws_operator_completeness", 463),
    ("ads_faction_rank",      25),
    ("ads_profession_profile", 8),
    ("ads_data_quality_summary", 9),
]

# ---- 主键唯一性：重复组数必须为 0 ----
PK_CHECKS = [
    ("dim_operator",           "operator_name"),
    ("dim_phase",              "phase_id"),
    ("dim_faction",            "operator_name, faction_name, relation"),
    ("dim_module",             "operator_name, module_name"),
    ("dim_tag",                "operator_name, tag"),
    ("dim_potential",          "operator_name, potential_level"),
    ("fact_operator_attr",     "operator_name, phase_id"),
    ("ods_prts_wikitext",      "operator_name"),
    ("ads_faction_rank",       "faction_name"),
    ("ads_profession_profile", "profession"),
]

# ---- 外键完整性：孤儿行数必须为 0 ----
FK_CHECKS = [
    ("fact_operator_attr.operator_name -> dim_operator", """
      SELECT COUNT(*) FROM fact_operator_attr f
      LEFT JOIN dim_operator o ON f.operator_name = o.operator_name
      WHERE o.operator_name IS NULL
    """),
    ("fact_operator_attr.phase_id -> dim_phase", """
      SELECT COUNT(*) FROM fact_operator_attr f
      LEFT JOIN dim_phase p ON f.phase_id = p.phase_id
      WHERE p.phase_id IS NULL
    """),
    ("dim_faction.operator_name -> dim_operator", """
      SELECT COUNT(*) FROM dim_faction f
      LEFT JOIN dim_operator o ON f.operator_name = o.operator_name
      WHERE o.operator_name IS NULL
    """),
    ("dim_tag.operator_name -> dim_operator", """
      SELECT COUNT(*) FROM dim_tag t
      LEFT JOIN dim_operator o ON t.operator_name = o.operator_name
      WHERE o.operator_name IS NULL
    """),
    ("dim_module.operator_name -> dim_operator", """
      SELECT COUNT(*) FROM dim_module m
      LEFT JOIN dim_operator o ON m.operator_name = o.operator_name
      WHERE o.operator_name IS NULL
    """),
    ("dim_potential.operator_name -> dim_operator", """
      SELECT COUNT(*) FROM dim_potential p
      LEFT JOIN dim_operator o ON p.operator_name = o.operator_name
      WHERE o.operator_name IS NULL
    """),
]

# ---- 跨层一致性 ----
CROSS_CHECKS = [
    ("ODS 有但 DWD 没有的干员", """
      SELECT COUNT(*) FROM (
        SELECT operator_name FROM ods_prts_wikitext
        EXCEPT
        SELECT operator_name FROM dim_operator
      )
    """),
    ("DWD 有但 ODS 没有的干员", """
      SELECT COUNT(*) FROM (
        SELECT operator_name FROM dim_operator
        EXCEPT
        SELECT operator_name FROM ods_prts_wikitext
      )
    """),
]

# ---- 取值合理性 ----
SANITY_CHECKS = [
    ("星级不在 1-6 的干员数", """
      SELECT COUNT(*) FROM dim_operator WHERE star IS NULL OR star < 1 OR star > 6
    """),
    ("事实表生命非正的记录数", """
      SELECT COUNT(*) FROM fact_operator_attr WHERE hp IS NULL OR hp <= 0
    """),
    ("事实表攻击非正的记录数", """
      SELECT COUNT(*) FROM fact_operator_attr WHERE atk IS NULL OR atk <= 0
    """),
    ("事实表部署费用非正的记录数", """
      SELECT COUNT(*) FROM fact_operator_attr WHERE cost IS NULL OR cost <= 0
    """),
    ("事实表阻挡数为负的记录数", """
      SELECT COUNT(*) FROM fact_operator_attr WHERE block < 0
    """),
    ("dim_operator 名称/职业为空的干员数", """
      SELECT COUNT(*) FROM dim_operator WHERE operator_name IS NULL OR profession IS NULL
    """),
    ("dim_faction 势力名为空的行数", """
      SELECT COUNT(*) FROM dim_faction WHERE faction_name IS NULL OR faction_name = ''
    """),
    ("dim_potential 非天赋类但无数值的行数", """
      SELECT COUNT(*) FROM dim_potential
      WHERE NOT is_special AND bonus_value IS NULL
    """),
    ("dim_module 八项加成全空的模组数", """
      SELECT COUNT(*) FROM dim_module
      WHERE bonus_atk IS NULL AND bonus_def IS NULL AND bonus_hp IS NULL
        AND bonus_res IS NULL AND bonus_block IS NULL AND bonus_attack_speed IS NULL
        AND bonus_cost IS NULL AND bonus_respawn_time IS NULL
    """),
    ("阶段档位不在 1-4 的事实行数", """
      SELECT COUNT(*) FROM fact_operator_attr WHERE phase_id NOT IN (1,2,3,4)
    """),
]

# ---- DWS/ADS 可加性抽查（用 DWS 现算 vs ADS 成品值）----
CONSISTENCY_CHECKS = [
    ("ADS 与 DWS 的职业平均攻击不一致的职业数", """
      SELECT COUNT(*) FROM (
        SELECT a.profession,
               a.avg_atk,
               ROUND(SUM(b.atk_sum)/SUM(b.atk_cnt), 1) AS dws_avg
        FROM ads_profession_profile a
        JOIN dws_profession_branch b
          ON a.profession = b.profession AND b.phase_id = 4
        GROUP BY a.profession, a.avg_atk
      ) WHERE ABS(avg_atk - dws_avg) > 0.05
    """),
]


def main():
    from pyspark.sql import SparkSession

    spark = (SparkSession.builder
             .appName("quality_check")
             .master("local[2]")
             .config("spark.sql.warehouse.dir",
                     "file:///" + os.path.join(ROOT, "spark-warehouse").replace("\\", "/"))
             .config("spark.driver.memory", "2g")
             .enableHiveSupport()
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql("USE arknights_dw")

    results = []   # (分组, 检查项, 实际值, 期望值, 是否通过)

    def add(group, name, actual, expected):
        ok = (actual == expected)
        results.append((group, name, actual, expected, ok))

    # ---- 1. 行数基线 ----
    print("=== 1. 行数基线 ===", flush=True)
    for table, exp in ROW_BASELINE:
        n = spark.table(table).count()
        add("行数", table, n, exp)
        print(f"   {'✅' if n == exp else '❌'} {table:<28} {n:>5} (期望 {exp})", flush=True)

    # ---- 2. 主键唯一性 ----
    print("\n=== 2. 主键唯一性（重复组数应为 0）===", flush=True)
    for table, cols in PK_CHECKS:
        sql = f"""
          SELECT COUNT(*) FROM (
            SELECT {cols}, COUNT(*) AS c
            FROM {table} GROUP BY {cols} HAVING c > 1
          )
        """
        dup = spark.sql(sql).collect()[0][0]
        add("主键", f"{table}({cols})", dup, 0)
        print(f"   {'✅' if dup == 0 else '❌'} {table:<28} 重复组 {dup}", flush=True)

    # ---- 3. 外键完整性 ----
    print("\n=== 3. 外键完整性（孤儿行应为 0）===", flush=True)
    for name, sql in FK_CHECKS:
        n = spark.sql(sql).collect()[0][0]
        add("外键", name, n, 0)
        print(f"   {'✅' if n == 0 else '❌'} {name:<52} 孤儿 {n}", flush=True)

    # ---- 4. 跨层一致性 ----
    print("\n=== 4. 跨层一致性（ODS 与 DWD 的干员集合）===", flush=True)
    for name, sql in CROSS_CHECKS:
        n = spark.sql(sql).collect()[0][0]
        add("跨层", name, n, 0)
        print(f"   {'✅' if n == 0 else '❌'} {name:<52} {n}", flush=True)

    # ---- 5. 取值合理性 ----
    print("\n=== 5. 取值合理性 ===", flush=True)
    for name, sql in SANITY_CHECKS:
        n = spark.sql(sql).collect()[0][0]
        add("合理性", name, n, 0)
        print(f"   {'✅' if n == 0 else '❌'} {name:<52} {n}", flush=True)

    # ---- 6. ADS 与 DWS 一致性 ----
    print("\n=== 6. ADS 与 DWS 一致性 ===", flush=True)
    for name, sql in CONSISTENCY_CHECKS:
        n = spark.sql(sql).collect()[0][0]
        add("一致性", name, n, 0)
        print(f"   {'✅' if n == 0 else '❌'} {name:<52} {n}", flush=True)

    # ---- 汇总 ----
    total = len(results)
    failed = [r for r in results if not r[4]]
    print("\n" + "=" * 60, flush=True)
    print(f"检查项合计 {total}，通过 {total - len(failed)}，失败 {len(failed)}", flush=True)
    if failed:
        print("\n失败明细：", flush=True)
        for g, name, actual, exp, _ in failed:
            print(f"   [{g}] {name}: 实际 {actual}，期望 {exp}", flush=True)
        print("\n❌ 数据质量校验未通过", flush=True)
        spark.stop()
        sys.exit(1)
    else:
        print("✅ 全部通过", flush=True)

    # 顺便把总表清单打出来，便于确认分层完整
    print("\n=== 表清单（按层）===", flush=True)
    tables = sorted([t.name for t in spark.catalog.listTables("arknights_dw")])
    for prefix in ("ods_", "dim_", "fact_", "dws_", "ads_"):
        group = [t for t in tables if t.startswith(prefix)]
        if group:
            tot = sum(spark.table(t).count() for t in group)
            print(f"   {prefix:<6} {len(group):>2} 张 / {tot:>6} 行   {', '.join(group)}", flush=True)

    spark.stop()


if __name__ == "__main__":
    main()
