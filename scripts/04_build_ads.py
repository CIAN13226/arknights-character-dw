# -*- coding: utf-8 -*-
"""建 ADS 应用层（3 张表），只从 DWS 读。

分层依赖：
    DWS(5 张表) -> 本脚本 -> ADS(3 张表)
    本脚本不碰 DWD，也不碰 ODS。

ADS 和 DWS 最大的区别：**ADS 可以存平均值和排名**。
    DWS 的下游还要再聚合，平均值存死就还原不回来；
    ADS 是链路终点，直接喂报表，没有下游再算一次。
    一句话：**不可加度量只允许出现在最后一层。**

口径统一说明：
    属性类指标（平均攻击/生命/费用）一律取 **精英2 满级（phase_id = 4）**，
    因为不同阶段的数值没有可比性，混在一起算平均没有意义。
    低星干员没有精英2，所以不参与属性平均——这会让人数少于总人数，属于预期。

预期行数：
    ads_faction_rank            约 20-40（有一级势力且到精英2的势力数）
    ads_profession_profile      8
    ads_data_quality_summary    约 10
"""
import os

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)

PHASE_E2 = 4          # 精英2 满级，属性类指标的统一口径

BUILDERS = [
    # ---------- 势力排行榜 ----------
    ("ads_faction_rank", f"""
      WITH base AS (
        SELECT
          faction_name,
          SUM(operator_cnt) AS operator_cnt,
          SUM(CASE WHEN star = 6 THEN operator_cnt ELSE 0 END) AS six_star_cnt,
          SUM(atk_sum)  AS atk_sum,  SUM(atk_cnt)  AS atk_cnt,
          SUM(hp_sum)   AS hp_sum,   SUM(hp_cnt)   AS hp_cnt,
          SUM(cost_sum) AS cost_sum, SUM(cost_cnt) AS cost_cnt
        FROM dws_faction_star
        WHERE phase_id = {PHASE_E2}
        GROUP BY faction_name
      )
      SELECT
        faction_name,
        ROW_NUMBER() OVER (ORDER BY operator_cnt DESC, faction_name) AS rank_no,
        operator_cnt,
        six_star_cnt,
        ROUND(atk_sum  / atk_cnt,  1) AS avg_atk,
        ROUND(hp_sum   / hp_cnt,   1) AS avg_hp,
        ROUND(cost_sum / cost_cnt, 2) AS avg_cost
      FROM base
      WHERE operator_cnt > 0
    """),

    # ---------- 职业画像 ----------
    ("ads_profession_profile", f"""
      WITH base AS (
        SELECT
          profession,
          COUNT(DISTINCT branch) AS branch_cnt,
          SUM(operator_cnt) AS operator_cnt,
          SUM(atk_sum)  AS atk_sum,  SUM(atk_cnt)  AS atk_cnt,
          SUM(hp_sum)   AS hp_sum,   SUM(hp_cnt)   AS hp_cnt,
          SUM(def_sum)  AS def_sum,  SUM(def_cnt)  AS def_cnt,
          SUM(cost_sum) AS cost_sum, SUM(cost_cnt) AS cost_cnt
        FROM dws_profession_branch
        WHERE phase_id = {PHASE_E2}
        GROUP BY profession
      )
      SELECT
        profession,
        operator_cnt,
        branch_cnt,
        ROUND(atk_sum  / atk_cnt,  1) AS avg_atk,
        ROUND(hp_sum   / hp_cnt,   1) AS avg_hp,
        ROUND(def_sum  / def_cnt,  1) AS avg_def,
        ROUND(cost_sum / cost_cnt, 2) AS avg_cost
      FROM base
      WHERE operator_cnt > 0
    """),

    # ---------- 数据质量总览（长表，一行一个指标，方便前端渲染）----------
    ("ads_data_quality_summary", """
      SELECT '干员总数' AS metric_name,
             CAST(COUNT(*) AS DOUBLE) AS metric_value,
             '来自 dim_operator' AS metric_note
      FROM dim_operator

      UNION ALL
      SELECT '有一级势力的干员数',
             CAST(COUNT(DISTINCT operator_name) AS DOUBLE),
             '来自 dim_faction，relation=所属 且 level=1'
      FROM dim_faction WHERE relation='所属' AND faction_level=1

      UNION ALL
      SELECT '有模组的干员数',
             CAST(COUNT(DISTINCT operator_name) AS DOUBLE),
             '来自 dim_module'
      FROM dim_module

      UNION ALL
      SELECT '有潜能的干员数',
             CAST(COUNT(DISTINCT operator_name) AS DOUBLE),
             '来自 dim_potential'
      FROM dim_potential

      UNION ALL
      SELECT '有缺失字段的干员数',
             CAST(SUM(CASE WHEN missing_cnt > 0 THEN 1 ELSE 0 END) AS DOUBLE),
             '来自 dws_operator_completeness，missing_cnt>0 计 1'
      FROM dws_operator_completeness

      UNION ALL
      SELECT '缺失字段总次数',
             CAST(SUM(missing_cnt) AS DOUBLE),
             '12 个描述字段的缺失次数之和'
      FROM dws_operator_completeness

      UNION ALL
      SELECT 'MOD 模组总条数',
             CAST(COUNT(*) AS DOUBLE),
             '来自 dim_module，一行一个干员的一个模组'
      FROM dim_module

      UNION ALL
      SELECT '潜能记录总条数',
             CAST(COUNT(*) AS DOUBLE),
             '来自 dim_potential'
      FROM dim_potential

      UNION ALL
      SELECT '事实表总行数',
             CAST(COUNT(*) AS DOUBLE),
             '来自 fact_operator_attr，一行一个干员一个阶段'
      FROM fact_operator_attr
    """),
]


def main():
    from pyspark.sql import SparkSession

    spark = (SparkSession.builder
             .appName("build_ads")
             .master("local[2]")
             .config("spark.sql.warehouse.dir",
                     "file:///" + os.path.join(ROOT, "spark-warehouse").replace("\\", "/"))
             .config("spark.driver.memory", "2g")
             .enableHiveSupport()
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql("USE arknights_dw")

    # 建表（DDL 里已定义，保险起见再执行一遍 IF NOT EXISTS）
    ddl = open(os.path.join(ROOT, "sql", "ddl.sql"), encoding="utf-8").read()
    for raw in ddl.split(";"):
        lines = [l for l in raw.splitlines() if not l.strip().startswith("--")]
        stmt = "\n".join(lines).strip()
        if stmt.upper().startswith("CREATE TABLE"):
            spark.sql(stmt)

    print("=== 构建 ADS ===", flush=True)
    for table, sql in BUILDERS:
        spark.sql(sql).write.mode("overwrite").insertInto(table)
        print(f"   {table:<28} {spark.table(table).count():>4} 行", flush=True)

    print("\n=== 势力排行榜（前 10）===", flush=True)
    spark.sql("""
      SELECT rank_no AS `名次`, faction_name AS `势力`, operator_cnt AS `人数`,
             six_star_cnt AS `六星`, avg_atk AS `平均攻击`,
             avg_hp AS `平均生命`, avg_cost AS `平均费用`
      FROM ads_faction_rank ORDER BY rank_no LIMIT 10
    """).show(truncate=False)

    print("=== 职业画像 ===", flush=True)
    spark.sql("""
      SELECT profession AS `职业`, operator_cnt AS `人数`, branch_cnt AS `分支数`,
             avg_atk AS `平均攻击`, avg_def AS `平均防御`, avg_cost AS `平均费用`
      FROM ads_profession_profile ORDER BY avg_atk DESC
    """).show(truncate=False)

    print("=== 数据质量总览 ===", flush=True)
    spark.sql("""
      SELECT metric_name AS `指标`, metric_value AS `值`, metric_note AS `口径`
      FROM ads_data_quality_summary
    """).show(truncate=False)

    print("=== 校验：ADS 的平均值必须等于 DWS 的 sum/cnt 现算值 ===", flush=True)
    spark.sql(f"""
      SELECT
        a.profession AS `职业`,
        a.avg_atk    AS `ADS平均攻击`,
        ROUND(SUM(b.atk_sum)/SUM(b.atk_cnt), 1) AS `DWS现算`,
        CASE WHEN ABS(a.avg_atk - ROUND(SUM(b.atk_sum)/SUM(b.atk_cnt),1)) < 0.05
             THEN 'OK' ELSE 'DIFF' END AS `结果`
      FROM ads_profession_profile a
      JOIN dws_profession_branch b
        ON a.profession = b.profession AND b.phase_id = {PHASE_E2}
      GROUP BY a.profession, a.avg_atk
      ORDER BY a.avg_atk DESC
    """).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
