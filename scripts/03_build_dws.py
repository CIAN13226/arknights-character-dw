# -*- coding: utf-8 -*-
"""建 DWS 汇总层（5 张表），只从 DWD 读。

分层依赖：
    DWD(7 张表) -> 本脚本 -> DWS(5 张表)
    本脚本不碰 ODS，也不碰任何源文件。

核心约束（DDL 注释里也写了）：
    只存可加度量（sum / count），不存平均值。
    平均 = SUM(x_sum) / SUM(x_cnt)，任意层级都能算对。

关于「势力是多值维度」：
    一个干员可能同时属于多个势力（如 凯尔希 = 罗德岛 + 巴别塔）。
    所以 dws_faction_star 里，同一个人会在多个势力下各计一次。
    这是多值维度的固有特性：**跨势力求和会重复计数**，
    看"全库总人数"要用 dim_operator，看"各势力人数"才用本表。

预期行数量级（不强制精确，因为取决于数据分布）：
    dws_faction_star            约 400-700
    dws_profession_branch       约 100-200
    dws_tag                     约 40-60
    dws_module_effect           约 8
    dws_operator_completeness   463
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)

# 每张表的构建 SQL。全部用纯 SQL 完成，不经过 Python 侧处理。
BUILDERS = [
    ("dws_faction_star", """
      SELECT
        f.faction_name,
        o.star,
        a.phase_id,
        COUNT(DISTINCT o.operator_name) AS operator_cnt,
        SUM(a.atk)  AS atk_sum,  COUNT(a.atk)  AS atk_cnt,
        SUM(a.hp)   AS hp_sum,   COUNT(a.hp)   AS hp_cnt,
        SUM(a.cost) AS cost_sum, COUNT(a.cost) AS cost_cnt
      FROM dim_faction f
      JOIN dim_operator o        ON f.operator_name = o.operator_name
      JOIN fact_operator_attr a  ON f.operator_name = a.operator_name
      WHERE f.relation = '所属' AND f.faction_level = 1
      GROUP BY f.faction_name, o.star, a.phase_id
    """),

    ("dws_profession_branch", """
      SELECT
        o.profession,
        o.branch,
        a.phase_id,
        COUNT(DISTINCT o.operator_name) AS operator_cnt,
        SUM(a.atk)  AS atk_sum,  COUNT(a.atk)  AS atk_cnt,
        SUM(a.hp)   AS hp_sum,   COUNT(a.hp)   AS hp_cnt,
        SUM(a.def)  AS def_sum,  COUNT(a.def)  AS def_cnt,
        SUM(a.cost) AS cost_sum, COUNT(a.cost) AS cost_cnt
      FROM dim_operator o
      JOIN fact_operator_attr a ON o.operator_name = a.operator_name
      GROUP BY o.profession, o.branch, a.phase_id
    """),

    ("dws_tag", """
      SELECT
        t.tag,
        COUNT(DISTINCT t.operator_name) AS operator_cnt,
        COUNT(DISTINCT CASE WHEN o.star = 6 THEN t.operator_name END) AS six_star_cnt,
        SUM(o.star)  AS star_sum,
        COUNT(o.star) AS star_cnt
      FROM dim_tag t
      JOIN dim_operator o ON t.operator_name = o.operator_name
      GROUP BY t.tag
    """),

    ("dws_module_effect", """
      SELECT
        o.profession,
        COUNT(*)                        AS module_cnt,
        COUNT(DISTINCT m.operator_name) AS operator_cnt,
        SUM(m.bonus_atk)          AS bonus_atk_sum,          COUNT(m.bonus_atk)          AS bonus_atk_cnt,
        SUM(m.bonus_hp)           AS bonus_hp_sum,           COUNT(m.bonus_hp)           AS bonus_hp_cnt,
        SUM(m.bonus_def)          AS bonus_def_sum,          COUNT(m.bonus_def)          AS bonus_def_cnt,
        SUM(m.bonus_attack_speed) AS bonus_attack_speed_sum, COUNT(m.bonus_attack_speed) AS bonus_attack_speed_cnt
      FROM dim_module m
      JOIN dim_operator o ON m.operator_name = o.operator_name
      GROUP BY o.profession
    """),

    # 数据完整度：不依赖事实表，只查各维度表有没有这个干员的记录
    ("dws_operator_completeness", """
      SELECT
        o.operator_name,
        o.star,
        o.profession,
        (CASE WHEN o.profession    IS NULL THEN 1 ELSE 0 END +
         CASE WHEN o.branch        IS NULL THEN 1 ELSE 0 END +
         CASE WHEN o.position      IS NULL THEN 1 ELSE 0 END +
         CASE WHEN o.race          IS NULL THEN 1 ELSE 0 END +
         CASE WHEN o.gender        IS NULL THEN 1 ELSE 0 END +
         CASE WHEN o.origin        IS NULL THEN 1 ELSE 0 END +
         CASE WHEN o.birthday      IS NULL THEN 1 ELSE 0 END +
         CASE WHEN o.height        IS NULL THEN 1 ELSE 0 END +
         CASE WHEN o.combat_exp    IS NULL THEN 1 ELSE 0 END +
         CASE WHEN o.trait         IS NULL THEN 1 ELSE 0 END +
         CASE WHEN o.attack_interval IS NULL THEN 1 ELSE 0 END +
         CASE WHEN o.redeploy      IS NULL THEN 1 ELSE 0 END) AS missing_cnt,
        12 AS total_cnt,
        CASE WHEN fa.operator_name IS NOT NULL THEN 1 ELSE 0 END AS has_faction,
        CASE WHEN tg.operator_name IS NOT NULL THEN 1 ELSE 0 END AS has_tag,
        CASE WHEN md.operator_name IS NOT NULL THEN 1 ELSE 0 END AS has_module,
        CASE WHEN pt.operator_name IS NOT NULL THEN 1 ELSE 0 END AS has_potential,
        CASE WHEN (COALESCE(o.trust_hp,0) + COALESCE(o.trust_atk,0) + COALESCE(o.trust_def,0)) > 0
             THEN 1 ELSE 0 END AS has_trust
      FROM dim_operator o
      LEFT JOIN (SELECT DISTINCT operator_name FROM dim_faction) fa ON o.operator_name = fa.operator_name
      LEFT JOIN (SELECT DISTINCT operator_name FROM dim_tag)     tg ON o.operator_name = tg.operator_name
      LEFT JOIN (SELECT DISTINCT operator_name FROM dim_module)  md ON o.operator_name = md.operator_name
      LEFT JOIN (SELECT DISTINCT operator_name FROM dim_potential) pt ON o.operator_name = pt.operator_name
    """),
]


def main():
    from pyspark.sql import SparkSession

    spark = (SparkSession.builder
             .appName("build_dws")
             .master("local[2]")
             .config("spark.sql.warehouse.dir",
                     "file:///" + os.path.join(ROOT, "spark-warehouse").replace("\\", "/"))
             .config("spark.driver.memory", "2g")
             .enableHiveSupport()
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql("USE arknights_dw")

    # 建表（DDL 里已定义，这里靠 01_create_tables.py；保险起见再跑一次 IF NOT EXISTS）
    ddl = open(os.path.join(ROOT, "sql", "ddl.sql"), encoding="utf-8").read()
    for raw in ddl.split(";"):
        lines = [l for l in raw.splitlines() if not l.strip().startswith("--")]
        stmt = "\n".join(lines).strip()
        if stmt.upper().startswith("CREATE TABLE"):
            spark.sql(stmt)

    print("=== 构建 DWS ===", flush=True)
    for table, sql in BUILDERS:
        df = spark.sql(sql)
        df.write.mode("overwrite").insertInto(table)
        n = spark.table(table).count()
        print(f"   {table:<28} {n:>5} 行", flush=True)

    # ---- 校验：可加性（DWS 汇总必须能还原 DWD）----
    #
    # 【重要】对账不能拿"全表求和"去比，两个表的口径本来就不一样：
    #   1. 势力表只覆盖「有一级势力」的干员，没有势力信息的干员被排除在外；
    #   2. 势力是多值维度，一个干员若属多个势力，会在每个势力下各计一次。
    # 所以正确的做法是**同口径对账**：挑一个具体势力，把它在 DWS 里的
    # 合计和"从 DWD 按同样筛选条件算出来的合计"比。
    print("\n=== 校验1：单势力同口径对账（罗德岛）===", flush=True)
    spark.sql("""
      SELECT
        (SELECT SUM(atk_sum) FROM dws_faction_star WHERE faction_name='罗德岛')
            AS `DWS_攻击合计`,
        (SELECT SUM(a.atk) FROM fact_operator_attr a
          WHERE a.operator_name IN (SELECT operator_name FROM dim_faction
                                    WHERE relation='所属' AND faction_name='罗德岛'))
            AS `DWD_同口径攻击合计`,
        (SELECT SUM(operator_cnt) FROM dws_faction_star WHERE faction_name='罗德岛')
            AS `DWS_人数x阶段`,
        (SELECT COUNT(*) FROM fact_operator_attr a
          WHERE a.operator_name IN (SELECT operator_name FROM dim_faction
                                    WHERE relation='所属' AND faction_name='罗德岛'))
            AS `DWD_同口径事实行数`
    """).show(truncate=False)

    print("=== 校验2：职业表可以严格对账（profession 是单值，join 不放大）===", flush=True)
    spark.sql("""
      SELECT
        (SELECT SUM(operator_cnt) FROM dws_profession_branch) AS `DWS_人数合计`,
        (SELECT COUNT(*) FROM fact_operator_attr)             AS `DWD_事实行数`,
        (SELECT SUM(atk_sum) FROM dws_profession_branch)      AS `DWS_攻击合计`,
        (SELECT SUM(atk) FROM fact_operator_attr)             AS `DWD_攻击合计`
    """).show(truncate=False)

    print("=== 校验3：为什么势力表的全局求和不可比 ===", flush=True)
    spark.sql("""
      SELECT
        (SELECT COUNT(DISTINCT operator_name) FROM dim_faction
          WHERE relation='所属' AND faction_level=1) AS `有一级势力的干员`,
        (SELECT COUNT(*) FROM dim_operator)          AS `全部干员`,
        (SELECT SUM(operator_cnt) FROM dws_faction_star) AS `势力表人数x阶段合计`
    """).show(truncate=False)
    print("-- 各职业在精英2满级的平均攻击，由 DWS 现算 --", flush=True)
    spark.sql("""
      SELECT profession AS `职业`,
             SUM(operator_cnt)          AS `人数`,
             ROUND(SUM(atk_sum)/SUM(atk_cnt), 1) AS `平均攻击`
      FROM dws_profession_branch
      WHERE phase_id = 4
      GROUP BY profession
      ORDER BY `平均攻击` DESC
    """).show(truncate=False)

    print("-- 同样口径直接从 DWD 算，结果应该完全一致 --", flush=True)
    spark.sql("""
      SELECT o.profession AS `职业`,
             ROUND(AVG(a.atk), 1) AS `平均攻击`
      FROM fact_operator_attr a
      JOIN dim_operator o ON a.operator_name = o.operator_name
      WHERE a.phase_id = 4
      GROUP BY o.profession
      ORDER BY `平均攻击` DESC
    """).show(truncate=False)

    print("=== 校验4：数据完整度 ===", flush=True)
    spark.sql("""
      SELECT
        SUM(missing_cnt)                      AS `缺失字段总数`,
        SUM(CASE WHEN missing_cnt > 0 THEN 1 ELSE 0 END) AS `有缺失的干员数`,
        SUM(has_faction)   AS `有势力`,
        SUM(has_tag)       AS `有标签`,
        SUM(has_module)    AS `有模组`,
        SUM(has_potential) AS `有潜能`,
        SUM(has_trust)     AS `有信赖`
      FROM dws_operator_completeness
    """).show(truncate=False)

    print("缺失最多的 5 个干员：", flush=True)
    spark.sql("""
      SELECT operator_name AS `干员`, star AS `星级`, missing_cnt AS `缺失数`,
             has_faction AS `有势力`, has_potential AS `有潜能`
      FROM dws_operator_completeness
      ORDER BY missing_cnt DESC, operator_name LIMIT 5
    """).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
