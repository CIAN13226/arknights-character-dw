# -*- coding: utf-8 -*-
"""建库 + 建 7 张表 + 灌 dim_phase。运行前必须设 HADOOP_HOME。"""
import os
from pyspark.sql import SparkSession

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
DDL = os.path.join(ROOT, "sql", "ddl.sql")

spark = (SparkSession.builder
         .appName("arknights_dw")
         .master("local[2]")
         .config("spark.sql.warehouse.dir",
                 "file:///" + os.path.join(ROOT, "spark-warehouse").replace("\\", "/"))
         .config("spark.driver.memory", "2g")
         .enableHiveSupport()  # 让元数据持久化到 Derby
         .getOrCreate())

spark.sparkContext.setLogLevel("ERROR")

spark.sql("CREATE DATABASE IF NOT EXISTS arknights_dw")
spark.sql("USE arknights_dw")

sql_text = open(DDL, encoding="utf-8").read()
for raw in sql_text.split(";"):
    lines = [l for l in raw.splitlines() if not l.strip().startswith("--")]
    stmt = "\n".join(lines).strip()
    if not stmt:
        continue
    print(">>>", stmt.split("(")[0].strip())
    spark.sql(stmt)

spark.sql("SHOW TABLES").show(truncate=False)

spark.sql("""INSERT INTO dim_phase VALUES
  (1,'精英0','1级'),(2,'精英0','满级'),
  (3,'精英1','满级'),(4,'精英2','满级')""")
spark.sql("SELECT * FROM dim_phase ORDER BY phase_id").show()

spark.stop()