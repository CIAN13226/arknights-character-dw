# -*- coding: utf-8 -*-
"""轻量 DAG 执行器：把 6 个脚本按依赖顺序串成一条流水线。

为什么不直接用 Airflow / DolphinScheduler：
    Airflow 官方不支持 Windows，需要通过 WSL 或 Docker 运行；本机两者都没有。
    DolphinScheduler 需要 ZooKeeper + 元数据库 + 中心节点，对单机练习来说过重。
    所以这里实现一个最小可用的 DAG 执行器，把调度器真正在做的几件事做出来：

        1. 声明式任务定义（id / 依赖 / 超时 / 重试）
        2. 拓扑排序执行（依赖先跑）
        3. 环检测（A→B→C→A 直接报错，不许提交）
        4. 失败传播：任一任务失败，它的所有下游 SKIPPED（等价 Airflow 的 all_success）
        5. 每次运行独立目录，逐任务落日志，最后写 summary.json
        6. 任务级重试
        7. 退出码：有失败则 1，可直接被外层再包一层或接 CI

和 Airflow 的概念对应关系：
    本文件            Airflow
    --------          --------------------------------
    Task(id=...)      Operator / @task
    deps=[...]        >> 依赖（上游 set_downstream）
    timeout / retries 同名字段
    SKIPPED           trigger_rule='all_success' 的效果
    _runs/<ts>/       TaskInstance 的日志目录
    --from / --only   airflow dags backfill / test 的部分能力

用法：
    python scripts\\run_pipeline.py --list        # 看任务清单和依赖
    python scripts\\run_pipeline.py --dry-run     # 只看执行计划，不真的跑
    python scripts\\run_pipeline.py               # 全量跑
    python scripts\\run_pipeline.py --from build_dwd   # 从某个任务开始（含）
    python scripts\\run_pipeline.py --only quality_check
    python scripts\\run_pipeline.py --self-test   # 自测拓扑/环检测/失败传播
"""
import argparse
import datetime
import json
import os
import subprocess
import sys
import time
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
PY = sys.executable

# 所有脚本都要求在项目根目录下运行 —— Derby 元数据库（metastore_db）是按工作目录
# 定位的，换个目录跑就会找不到库。这一点必须由执行器统一保证，不能靠人记。
WORKDIR = ROOT


class Task:
    def __init__(self, id, script, deps=(), timeout=1800, retries=0, desc=""):
        self.id = id
        self.script = script
        self.deps = list(deps)
        self.timeout = timeout
        self.retries = retries
        self.desc = desc

    def __repr__(self):
        return f"<Task {self.id}>"


# 依赖顺序是真实约束，不是为了好看：
#   create_tables 建库建表；00 脚本要用 USE arknights_dw，所以必须先有库
#   fetch_ods 抓原始数据进 ODS；DWD 从这里读
#   DWS 只从 DWD 读；ADS 只从 DWS 读；质量校验最后做全量体检
PIPELINE = [
    Task("create_tables", "01_create_tables.py", deps=[],
         timeout=900, desc="建库 + 建 16 张表 + 灌 dim_phase"),
    Task("fetch_ods", "00_fetch_wikitext_to_ods.py", deps=["create_tables"],
         timeout=2400, retries=1, desc="抓 463 份 wikitext 进 ODS（约 13 分钟）"),
    Task("build_dwd", "02_build_dwd_from_ods.py", deps=["fetch_ods"],
         timeout=1800, desc="从 ODS 重建 6 张 DWD 表"),
    Task("build_dws", "03_build_dws.py", deps=["build_dwd"],
         timeout=1200, desc="从 DWD 建 5 张 DWS 汇总表"),
    Task("build_ads", "04_build_ads.py", deps=["build_dws"],
         timeout=1200, desc="从 DWS 建 3 张 ADS 应用表"),
    Task("quality_check", "05_quality_check.py", deps=["build_ads"],
         timeout=900, desc="45 项数据质量校验，失败则整条流水线失败"),
]

STATUS_ICON = {"SUCCESS": "✅", "FAILED": "❌", "SKIPPED": "⏭️", "RUNNING": "▶️"}


# ---------------- DAG 逻辑 ----------------

def validate(tasks):
    """检查依赖存在性与环。返回 id -> Task 的字典。"""
    by_id = {t.id: t for t in tasks}
    if len(by_id) != len(tasks):
        raise ValueError("任务 id 重复")
    for t in tasks:
        for d in t.deps:
            if d not in by_id:
                raise ValueError(f"任务 {t.id} 依赖了不存在的 {d}")
    # 环检测：DFS 三色标记
    WHITE, GRAY, BLACK = 0, 1, 2
    color = {t.id: WHITE for t in tasks}
    stack = []

    def dfs(node):
        color[node] = GRAY
        stack.append(node)
        for d in by_id[node].deps:
            if color[d] == GRAY:
                cyc = " → ".join(stack[stack.index(d):] + [d])
                raise ValueError(f"检测到环: {cyc}")
            if color[d] == WHITE:
                dfs(d)
        stack.pop()
        color[node] = BLACK

    for t in tasks:
        if color[t.id] == WHITE:
            dfs(t.id)
    return by_id


def topo_order(tasks):
    """返回满足依赖的执行顺序（同层按定义顺序，保证可重复）。"""
    by_id = validate(tasks)
    order, done = [], set()
    remaining = list(tasks)
    while remaining:
        progressed = False
        for t in list(remaining):
            if all(d in done for d in t.deps):
                order.append(t)
                done.add(t.id)
                remaining.remove(t)
                progressed = True
        if not progressed:
            raise ValueError("无法排序，依赖可能有环")
    return order


def select(tasks, start=None, only=None):
    """支持 --from / --only。--from 会连带它的下游一起选。"""
    order = topo_order(tasks)
    if only:
        picked = [t for t in order if t.id == only]
        if not picked:
            raise ValueError(f"没有 id 为 {only} 的任务")
        return picked
    if not start:
        return order
    ids = [t.id for t in order]
    if start not in ids:
        raise ValueError(f"没有 id 为 {start} 的任务")
    # 从 start 起，以及所有（直接或间接）依赖 start 的任务
    by_id = {t.id: t for t in tasks}
    keep = {start}
    changed = True
    while changed:
        changed = False
        for t in tasks:
            if t.id not in keep and any(d in keep for d in t.deps):
                keep.add(t.id)
                changed = True
    return [t for t in order if t.id in keep]


# ---------------- 执行 ----------------

def build_env():
    """统一构造子进程环境：HADOOP_HOME、UTF-8、无缓冲。"""
    env = os.environ.copy()
    if not env.get("HADOOP_HOME"):
        candidate = r"E:\hadoop"
        if os.path.isdir(candidate):
            env["HADOOP_HOME"] = candidate
    hadoop_bin = os.path.join(env.get("HADOOP_HOME", ""), "bin")
    if os.path.isdir(hadoop_bin):
        env["PATH"] = hadoop_bin + os.pathsep + env.get("PATH", "")
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    return env


def run_script(task, log_dir, env):
    """真正执行一个任务：跑脚本、超时控制、返回 (状态, 退出码, 耗时, 日志路径)。"""
    log_path = os.path.join(log_dir, f"{task.id}.log")
    script = os.path.join(HERE, task.script)
    if not os.path.exists(script):
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(f"脚本不存在: {script}\n")
        return "FAILED", -1, 0.0, log_path

    t0 = time.time()
    with open(log_path, "w", encoding="utf-8") as f:
        f.write(f"# task={task.id}\n# script={task.script}\n"
                f"# cwd={WORKDIR}\n# start={datetime.datetime.now().isoformat()}\n"
                + "-" * 60 + "\n")
        f.flush()
        try:
            proc = subprocess.run([PY, "-u", script], cwd=WORKDIR, env=env,
                                  stdout=f, stderr=subprocess.STDOUT,
                                  timeout=task.timeout)
            code = proc.returncode
        except subprocess.TimeoutExpired:
            f.write(f"\n[超时] 超过 {task.timeout}s，已终止\n")
            return "FAILED", -9, time.time() - t0, log_path
        except Exception:
            f.write("\n[执行器异常]\n" + traceback.format_exc())
            return "FAILED", -1, time.time() - t0, log_path
    return ("SUCCESS" if code == 0 else "FAILED"), code, time.time() - t0, log_path


def execute(tasks, log_dir, executor=None, verbose=True):
    """按拓扑序执行。executor 可注入，便于自测。
    返回 (结果列表, 总体是否成功)。

    关于「不在本次范围内的上游依赖」：
        --from / --only 的语义是"上游已经跑过了，我从这里接着跑"。
        所以范围之外的依赖视为已满足，否则 --from build_dwd 会永远被
        fetch_ods 挡住（这个 bug 是实际跑一遍才发现的）。
        为了不让人误解，被假设的上游会在输出里标出来。
    """
    env = build_env()
    executor = executor or (lambda t: run_script(t, log_dir, env))
    results, status = [], {}

    in_scope = {t.id for t in tasks}
    assumed = sorted({d for t in tasks for d in t.deps if d not in in_scope})
    if assumed and verbose:
        print(f"   注意：以下上游不在本次范围内，视为已完成 -> {', '.join(assumed)}\n", flush=True)

    for t in tasks:
        # 只看范围内的依赖：范围内必须 SUCCESS，范围外视为已满足
        blocked = [d for d in t.deps if d in in_scope and status.get(d) != "SUCCESS"]
        if blocked:
            status[t.id] = "SKIPPED"
            results.append({"task": t.id, "status": "SKIPPED", "exit_code": None,
                            "seconds": 0.0, "log": None,
                            "reason": "上游未成功: " + ", ".join(blocked)})
            if verbose:
                print(f"   {STATUS_ICON['SKIPPED']} {t.id:<14} 跳过（上游未成功: {', '.join(blocked)}）",
                      flush=True)
            continue

        ext = [d for d in t.deps if d not in in_scope]
        if ext and verbose and t is tasks[0]:
            print(f"      （{t.id} 的上游 {', '.join(ext)} 视为已完成）", flush=True)

        attempt = 0
        while True:
            attempt += 1
            if verbose:
                note = f"（第 {attempt} 次尝试）" if attempt > 1 or t.retries else ""
                print(f"   {STATUS_ICON['RUNNING']} {t.id:<14} 开始{note}", flush=True)
            st, code, secs, log = executor(t)
            if st == "SUCCESS" or attempt > t.retries:
                break
            if verbose:
                print(f"      ↻ 失败（退出码 {code}），重试 {attempt}/{t.retries}", flush=True)
            time.sleep(2)

        status[t.id] = st
        results.append({"task": t.id, "status": st, "exit_code": code,
                        "seconds": round(secs, 1), "log": log,
                        "attempts": attempt,
                        "assumed_upstream": ext})
        if verbose:
            print(f"   {STATUS_ICON.get(st,'?')} {t.id:<14} {st:<8} {secs:.1f}s  日志 {os.path.basename(log or '')}",
                  flush=True)

    ok = all(r["status"] == "SUCCESS" for r in results) and len(results) == len(tasks)
    return results, ok


# ---------------- 自测 ----------------

def self_test():
    """不碰真实数据，验证 DAG 逻辑的三件事。"""
    print("=== 自测 ===", flush=True)
    ok = True

    diamond = [
        Task("a", "x.py", deps=[]),
        Task("b", "x.py", deps=["a"]),
        Task("c", "x.py", deps=["a"]),
        Task("d", "x.py", deps=["b", "c"]),
    ]
    order = [t.id for t in topo_order(diamond)]
    print(f"1) 菱形依赖的拓扑序: {order}")
    if order.index("a") == 0 and order.index("d") == len(order) - 1:
        print("   ✅ a 在最前、d 在最后")
    else:
        print("   ❌ 顺序不对"); ok = False

    try:
        validate([Task("x", "x.py", deps=["y"]), Task("y", "x.py", deps=["x"])])
        print("2) ❌ 环没被检测出来"); ok = False
    except ValueError as e:
        print(f"2) ✅ 环检测生效: {e}")

    # 失败传播：b 失败 -> d（依赖 b）应被 SKIPPED
    def stub(t):
        if t.id == "b":
            return "FAILED", 1, 0.1, None
        return "SUCCESS", 0, 0.1, None

    import tempfile
    with tempfile.TemporaryDirectory() as td:
        results, all_ok = execute(diamond, td, executor=stub, verbose=False)
    st = {r["task"]: r["status"] for r in results}
    print(f"3) 失败传播结果: {st}，整体成功={all_ok}")
    if st.get("b") == "FAILED" and st.get("d") == "SKIPPED" and not all_ok:
        print("   ✅ 失败任务的下游被跳过，整体判为失败")
    else:
        print("   ❌ 失败传播不对"); ok = False

    print("\n" + ("✅ 自测全部通过" if ok else "❌ 自测未通过"), flush=True)
    return 0 if ok else 1


# ---------------- 入口 ----------------

def main():
    ap = argparse.ArgumentParser(description="arknights_dw 轻量 DAG 执行器")
    ap.add_argument("--list", action="store_true", help="列出任务与依赖")
    ap.add_argument("--dry-run", action="store_true", help="只打印执行计划")
    ap.add_argument("--from", dest="start", help="从该任务开始（含其下游）")
    ap.add_argument("--only", help="只跑这一个任务")
    ap.add_argument("--self-test", action="store_true", help="自测 DAG 逻辑")
    args = ap.parse_args()

    if args.self_test:
        return self_test()

    validate(PIPELINE)

    if args.list:
        print("任务清单（按执行顺序）:")
        for i, t in enumerate(topo_order(PIPELINE), 1):
            dep = ", ".join(t.deps) or "—"
            retry = f" 重试{t.retries}" if t.retries else ""
            print(f"  {i}. {t.id:<14} 依赖: {dep:<16} 超时 {t.timeout}s{retry}")
            print(f"     {t.desc}")
        return 0

    try:
        selected = select(PIPELINE, start=args.start, only=args.only)
    except ValueError as e:
        print(f"参数错误: {e}", file=sys.stderr)
        return 2

    if args.dry_run:
        print("执行计划（dry-run，不会真的运行）:")
        for i, t in enumerate(selected, 1):
            dep = ", ".join(t.deps) or "—"
            print(f"  {i}. {t.id:<14} 依赖: {dep}  超时 {t.timeout}s"
                  + (f"  重试 {t.retries}" if t.retries else ""))
        print(f"\n共 {len(selected)} 个任务")
        return 0

    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = os.path.join(ROOT, "_runs", ts)
    os.makedirs(run_dir, exist_ok=True)

    print(f"运行目录: {run_dir}", flush=True)
    print(f"任务数:  {len(selected)}", flush=True)
    print(f"工作目录: {WORKDIR}（元数据库按工作目录定位，必须固定）\n", flush=True)

    t_start = time.time()
    results, ok = execute(selected, run_dir)
    total = time.time() - t_start

    summary = {
        "run_id": ts,
        "started": ts,
        "seconds": round(total, 1),
        "success": ok,
        "tasks": results,
    }
    with open(os.path.join(run_dir, "summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("\n" + "=" * 62, flush=True)
    for r in results:
        icon = STATUS_ICON.get(r["status"], "?")
        line = f"{icon} {r['task']:<14} {r['status']:<8} {r['seconds']:>7.1f}s"
        if r.get("reason"):
            line += f"  ({r['reason']})"
        print(line, flush=True)
    print("=" * 62, flush=True)
    print(f"总耗时 {total:.1f}s   结果: {'✅ 全部成功' if ok else '❌ 存在失败'}", flush=True)
    print(f"运行清单: {os.path.join(run_dir, 'summary.json')}", flush=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
