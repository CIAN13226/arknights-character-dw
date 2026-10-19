# -*- coding: utf-8 -*-
"""灌 dim_module（513 行）—— 模组维度。

数据源：corpus_db/<name>.md 属性段的「- 模组：」行，形如

    - 模组：Mon2tr（atk:60;def:25）；医者（max_hp:195;attack_speed:7）；凯尔希特限证章（atk:50;attack_speed:7）

分隔符有三层，必须分清楚：
    模组之间  ->  ；  全角分号
    键值之间  ->  ;   半角分号
    键和值     ->  :   半角冒号

全库只有 8 种键名（已扫过）：
    atk               459 次  -> bonus_atk
    max_hp            371 次  -> bonus_hp
    def               200 次  -> bonus_def
    attack_speed       67 次  -> bonus_attack_speed   （攻速加成 S，越大越快）
    cost                9 次  -> bonus_cost
    magic_resistance    8 次  -> bonus_res            （注意不叫 res）
    respawn_time        6 次  -> bonus_respawn_time
    block_cnt           5 次  -> bonus_block

有意思的一点：全库**没有**任何模组改「攻击间隔」，所以 DDL 里原来那列 bonus_interval
是凭印象加的，已经删掉。改由 bonus_cost / bonus_respawn_time 替代。

另外，respawn_time 这 6 条正好解答了一个疑问：
之前以为「处决者/情报官的再部署随精英阶段变」，但全库再部署都是单值（无链式）。
真相是——它们改的是**模组**，不是精英阶段。数据对上了。

初始模组（如「凯尔希证章」）没有任何加成数据，不进表。
398 个干员有模组，共 513 条。
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
TMP_JSON = os.path.join(TMP_DIR, "dim_module.json")

SCHEMA = [
    ("operator_name", "string"), ("module_name", "string"),
    ("bonus_atk", "int"), ("bonus_def", "int"), ("bonus_hp", "int"), ("bonus_res", "int"),
    ("bonus_block", "int"), ("bonus_attack_speed", "int"),
    ("bonus_cost", "int"), ("bonus_respawn_time", "int"),
]

# 原始键名 -> 列名。没在这张表里的键，打警告，不静默丢
KEY_MAP = {
    "atk": "bonus_atk",
    "def": "bonus_def",
    "max_hp": "bonus_hp",
    "magic_resistance": "bonus_res",
    "block_cnt": "bonus_block",
    "attack_speed": "bonus_attack_speed",
    "cost": "bonus_cost",
    "respawn_time": "bonus_respawn_time",
}

MODULE_RE = re.compile(r"^(.+?)（(.+)）\s*$")


def main():
    rows = []
    unknown_keys, bad_format = [], []
    ops_with = 0

    for fn in os.listdir(CORPUS):
        if not fn.endswith(".md"):
            continue
        name = fn[:-3]
        text = open(os.path.join(CORPUS, fn), encoding="utf-8").read()

        i = text.find("## 属性")
        if i < 0:
            continue
        j = text.find("\n## ", i + 5)
        seg = text[i: j if j > 0 else len(text)]

        m = re.search(r"模组：(.+)", seg)
        if not m:
            continue
        ops_with += 1

        # 按全角分号切出每个模组
        for chunk in re.split(r"；", m.group(1)):
            chunk = chunk.strip()
            if not chunk:
                continue
            mm = MODULE_RE.match(chunk)
            if not mm:
                bad_format.append((name, chunk))
                continue
            mod_name, bonus = mm.group(1).strip(), mm.group(2)

            row = {"operator_name": name, "module_name": mod_name}
            for col, _ in SCHEMA:
                row.setdefault(col, None)

            for kv in bonus.split(";"):
                kv = kv.strip()
                if not kv:
                    continue
                parts = re.split(r"[:：]", kv, maxsplit=1)
                if len(parts) != 2:
                    bad_format.append((name, chunk))
                    continue
                k, v = parts[0].strip(), parts[1].strip()
                col = KEY_MAP.get(k)
                if col is None:
                    unknown_keys.append((name, mod_name, k, v))
                    continue
                try:
                    row[col] = int(v)
                except ValueError:
                    bad_format.append((name, chunk))
            rows.append(row)

    print(f"有模组的干员: {ops_with}，拼装 {len(rows)} 行")
    if unknown_keys:
        print(f"⚠️ 未识别的键名 {len(unknown_keys)} 条（会被丢弃！）：")
        for n, mn, k, v in unknown_keys[:10]:
            print(f"   {n:<14} {mn} {k}:{v}")
    if bad_format:
        print(f"⚠️ 格式异常 {len(bad_format)} 条：")
        for n, c in bad_format[:6]:
            print(f"   {n:<14} {c}")

    os.makedirs(TMP_DIR, exist_ok=True)
    with open(TMP_JSON, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    spark = (SparkSession.builder
             .appName("load_dim_module")
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
      .write.mode("overwrite").insertInto("dim_module")
    print(f"写入 dim_module: {spark.table('dim_module').count()} 行")

    print("\n=== 校验 ===")
    # 每种加成有多少条非空
    spark.sql("""
      SELECT
        COUNT(*)                                                      AS `总行数`,
        COUNT(DISTINCT operator_name)                                 AS `干员数`,
        SUM(CASE WHEN bonus_atk    IS NOT NULL THEN 1 ELSE 0 END)     AS `攻击`,
        SUM(CASE WHEN bonus_hp     IS NOT NULL THEN 1 ELSE 0 END)     AS `生命`,
        SUM(CASE WHEN bonus_def    IS NOT NULL THEN 1 ELSE 0 END)     AS `防御`,
        SUM(CASE WHEN bonus_attack_speed IS NOT NULL THEN 1 ELSE 0 END) AS `攻速`,
        SUM(CASE WHEN bonus_cost   IS NOT NULL THEN 1 ELSE 0 END)     AS `费用`,
        SUM(CASE WHEN bonus_res    IS NOT NULL THEN 1 ELSE 0 END)     AS `法抗`,
        SUM(CASE WHEN bonus_respawn_time IS NOT NULL THEN 1 ELSE 0 END) AS `再部署`,
        SUM(CASE WHEN bonus_block  IS NOT NULL THEN 1 ELSE 0 END)     AS `阻挡`
      FROM dim_module
    """).show(truncate=False)

    print("凯尔希的 3 个模组（Mon2tr 攻60防25 / 医者 生命195攻速7 / 特限证章 攻50攻速7）：")
    spark.sql("""
      SELECT module_name AS `模组`, bonus_atk AS `攻`, bonus_def AS `防`,
             bonus_hp AS `生命`, bonus_attack_speed AS `攻速`
      FROM dim_module WHERE operator_name = '凯尔希' ORDER BY module_name
    """).show(truncate=False)

    print("改了再部署时间的模组（应该是处决者/情报官一类）：")
    spark.sql("""
      SELECT m.operator_name AS `干员`, m.module_name AS `模组`
      FROM dim_module m WHERE m.bonus_respawn_time IS NOT NULL
    """).show(truncate=False)

    print("加了阻挡的模组：")
    spark.sql("""
      SELECT operator_name AS `干员`, module_name AS `模组`, bonus_block AS `阻挡加成`
      FROM dim_module WHERE bonus_block IS NOT NULL
    """).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
