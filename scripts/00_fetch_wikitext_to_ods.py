# -*- coding: utf-8 -*-
"""ODS 层：把 PRTS 的 wikitext 原文抓下来落地（带断点续跑）。

为什么 ODS 存 wikitext 而不是 operators.json / corpus_db：
    那两个是 RAG 项目已经加工过的产物。数仓如果吃它们的产物，
    就等于依赖上游项目，分层也只是形式。
    这里让数仓直接从 wiki 原始源码开始，DWD 自己解析。

ODS 的铁律：
    1. 不加工：wikitext 一个字都不改
    2. 不判断类型：全存 STRING
    3. 留元数据：etl_date / source_url / char_len

三个工程细节（第一版没有，导致跑起来无法监控）：
    1. print(..., flush=True) —— 输出被管道缓冲后看不到进度
    2. 每 50 个就把已抓的追加写进 JSONL —— 中途崩了不用从头再来
    3. 断点续跑：启动时先读已有 JSONL，跳过已抓过的干员

重试：单个干员失败重试 3 次，间隔 5 秒（按要求）。

用法：
    $env:HADOOP_HOME = "E:\hadoop"; $env:PATH = "E:\hadoop\bin;$env:PATH"
    cd arknights_dw
    python scripts\\00_fetch_wikitext_to_ods.py

    想从头重抓：先删 _tmp/ods_prts_wikitext.jsonl
"""
import os
import sys
import time
import json
import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
RAG = os.path.join(ROOT, "..", "rag_project")
TMP_DIR = os.path.join(ROOT, "_tmp")
PROGRESS = os.path.join(TMP_DIR, "ods_prts_wikitext.jsonl")   # 增量落盘（断点续跑用）

sys.path.insert(0, RAG)
import fetch_prts as fp          # noqa: E402

RETRY = 3
RETRY_WAIT = 5
SLEEP = 1.0          # 正常请求之间间隔，别打疼 wiki
FLUSH_EVERY = 50     # 每抓多少个落一次盘


def log(msg):
    """带刷新的打印，避免被管道缓冲看不到。"""
    print(msg, flush=True)


def fetch_one(name):
    last_err = None
    for attempt in range(1, RETRY + 1):
        try:
            wt = fp.get_wikitext(name)
            if wt:
                return wt, None
            last_err = "返回空"
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
        if attempt < RETRY:
            log(f"      {name} 第 {attempt} 次失败（{last_err}），{RETRY_WAIT}s 后重试")
            time.sleep(RETRY_WAIT)
    return None, last_err


def load_done():
    """读已抓好的，用于断点续跑。"""
    done = {}
    if os.path.exists(PROGRESS):
        with open(PROGRESS, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                    done[r["operator_name"]] = r
                except Exception:
                    pass
    return done


def main():
    etl_date = datetime.date.today().isoformat()
    os.makedirs(TMP_DIR, exist_ok=True)

    titles = [t for t in fp.list_operators(500) if t]
    log(f"抽取日期 {etl_date}；干员列表 {len(titles)} 个")

    done = load_done()
    log(f"已有 {len(done)} 个（断点续跑）")

    todo = [t for t in titles if t not in done]
    log(f"待抓 {len(todo)} 个\n")

    failed, buf = [], []
    t_start = time.time()

    # 追加模式打开，边抓边写
    with open(PROGRESS, "a", encoding="utf-8") as fout:
        for i, name in enumerate(todo, 1):
            wt, err = fetch_one(name)
            if wt is None:
                failed.append((name, err))
                log(f"[{i}/{len(todo)}] ❌ {name}  {err}")
            else:
                rec = {
                    "operator_name": name,
                    "wikitext": wt,
                    "char_len": len(wt),
                    "etl_date": etl_date,
                    "source_url": "https://prts.wiki/" + name.replace(" ", "_"),
                }
                buf.append(rec)
                fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
                if len(buf) >= FLUSH_EVERY:
                    fout.flush()
                    el = time.time() - t_start
                    log(f"[{i}/{len(todo)}] 已抓 {len(done)+len(buf)} 个，本轮 {el:.0f}s")
                    buf = []
            time.sleep(SLEEP)

        fout.flush()

    el = time.time() - t_start
    total = len(done) + len(todo) - len(failed)
    log(f"\n抓取结束：本轮新增 {len(todo)-len(failed)}，累计 {total}，失败 {len(failed)}，用时 {el:.0f}s")
    if failed:
        log("失败清单：")
        for n, e in failed:
            log(f"   {n:<20} {e}")
    if failed:
        log("\n⚠️ 有失败，先不建表。修好网络后重跑本脚本即可续抓（已抓的会跳过）。")
        return

    # ---- 全部就绪，重建 JSONL 并写 ODS 表 ----
    rows = list(load_done().values())
    log(f"\n准备写库：{len(rows)} 行")

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = (SparkSession.builder
             .appName("load_ods_wikitext")
             .master("local[2]")
             .config("spark.sql.warehouse.dir",
                     "file:///" + os.path.join(ROOT, "spark-warehouse").replace("\\", "/"))
             .config("spark.driver.memory", "2g")
             .enableHiveSupport()
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql("USE arknights_dw")

    spark.sql("""
      CREATE TABLE IF NOT EXISTS ods_prts_wikitext (
        operator_name  STRING NOT NULL COMMENT '干员名，同时也是 PRTS 页面标题',
        wikitext       STRING         COMMENT '页面 wikitext 原文，未做任何加工',
        char_len       INT            COMMENT '字符数，用于校验入库无截断',
        etl_date       STRING         COMMENT '抽取日期 yyyy-MM-dd',
        source_url     STRING         COMMENT '来源 URL'
      ) USING PARQUET
    """)

    SCHEMA = [("operator_name", "string"), ("wikitext", "string"),
              ("char_len", "int"), ("etl_date", "string"), ("source_url", "string")]
    df = spark.read.json(PROGRESS)
    df.select([F.col(c).cast(t).alias(c) for c, t in SCHEMA]) \
      .write.mode("overwrite").insertInto("ods_prts_wikitext")

    log("\n=== 校验 ===")
    spark.sql("""
      SELECT COUNT(*) AS `行数`,
             SUM(char_len) AS `总字符数`,
             MIN(char_len) AS `最短`,
             MAX(char_len) AS `最长`,
             SUM(CASE WHEN wikitext IS NULL OR wikitext = '' THEN 1 ELSE 0 END) AS `空内容`
      FROM ods_prts_wikitext
    """).show(truncate=False)

    mism = spark.sql("""
      SELECT COUNT(*) AS n FROM ods_prts_wikitext
      WHERE char_len <> LENGTH(wikitext)
    """).collect()[0][0]
    log(f"char_len 与 LENGTH(wikitext) 不一致的行数: {mism}（应为 0）")

    n_tpl = spark.sql("""
      SELECT COUNT(*) AS n FROM ods_prts_wikitext WHERE wikitext LIKE '%{{%'
    """).collect()[0][0]
    log(f"含 {{{{ 模板语法的行数: {n_tpl}（应为 463，证明存的是原始源码）")

    row = spark.sql("""
      SELECT operator_name, SUBSTR(wikitext,1,100) AS head
      FROM ods_prts_wikitext WHERE operator_name='凯尔希'
    """).collect()
    if row:
        log(f"凯尔希 wikitext 开头: {row[0]['head']!r}")

    spark.stop()


if __name__ == "__main__":
    main()
