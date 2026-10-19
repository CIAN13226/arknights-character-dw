# -*- coding: utf-8 -*-
"""DWD 层的解析模块：输入一段原始 wikitext，输出各张 DWD 表需要的数据结构。

为什么要单独一个模块：
    DWD 层的 5 个加载脚本都需要解析同一份 wikitext，各写一遍必然重复且容易不一致。
    把解析收口到这里，加载脚本只负责"取数据 → 调这里 → 写表"。

前提：ODS 表 ods_prts_wikitext 存的是 PRTS 页面 wikitext 原文。
     本模块的所有函数都只接收 wikitext 字符串，不碰文件、不碰数据库。

复用的解析逻辑来自 ../rag_project/fetch_prts.py 和 dump_operators.py，
那些函数本来就是 f(wikitext) 形式的纯函数，所以能直接拿来用。
额外补的一层是「档案里的【种族】/【性别】...」，因为那些字段不在 {{属性}} 模板里。

字段来源一览（DWD 各表要什么、从哪来）：
    dim_operator:
        名称/星级/职业/分支/站位/标签/特性   <- CharinfoV2 模板
        种族/性别/出身地/战斗经验/生日/身高  <- 干员档案的「基础档案」小节
        攻击间隔/再部署                     <- {{属性}} 模板
        信赖加成(生命/攻击/防御)             <- {{属性}} 模板
        精英等级上限                        <- {{属性}} 模板
    dim_faction:
        所属势力（层级）/ 隐藏势力          <- {{属性}} 模板
    dim_tag:
        标签                                <- CharinfoV2 模板（空格分隔，多值）
    dim_module:
        模组名 + 加成键值                   <- {{属性}} 模板
    dim_potential:
        潜能2~6 的效果                      <- ==潜能提升== 章节
    fact_operator_attr:
        各阶段 hp/atk/def/res + 阻挡数/费用 <- {{属性}} 模板
"""
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
RAG = os.path.join(ROOT, "..", "rag_project")
if RAG not in sys.path:
    sys.path.insert(0, RAG)

import fetch_prts as fp          # noqa: E402
import dump_operators as D       # noqa: E402


# ---------- 通用 ----------

ARCHIVE_FIELD_RE = re.compile(r"【(种族|性别|出身地|战斗经验|生日|身高)】\s*([^【】]+)")

# 属性段里各阶段行的匹配
PHASE_LINE_RE = re.compile(r"^精英(\d)\s*(1级|满级)[^：]*：(.*)$")
PHASE_ID = {(0, "1级"): 1, (0, "满级"): 2, (1, "满级"): 3, (2, "满级"): 4}

# 潜能行
POTENTIAL_LINE_RE = re.compile(r"^潜能(\d)：(.+)$")
POTENTIAL_TYPE_MAP = [
    ("部署费用",   "cost"),
    ("再部署时间", "re_deploy"),
    ("攻击力",     "atk"),
    ("防御力",     "def"),
    ("生命上限",   "hp"),
    ("最大生命值", "hp"),
    ("攻击速度",   "attack_speed"),
    ("法术抗性",   "res"),
]
POTENTIAL_SPECIAL = ("天赋效果增强", "天赋效果加强", "第一天赋效果增强", "第二天赋效果增强")
NUM_RE = re.compile(r"([+-])(\d+)")

# 模组加成键名
MODULE_KEY_MAP = {
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


def _sections(wt):
    return fp.split_sections(wt)


def _attr_lines(wt):
    """属性段的每一行（去掉 '## 属性' 标题）。复用 dump_operators.parse_attr。"""
    return D.parse_attr(wt) or []


# ---------- dim_operator ----------

def parse_operator(wt):
    """返回 dim_operator 一行的 dict（不含 operator_name，由调用方给）。"""
    row = {}

    # 1) CharinfoV2：名称/星级/职业/分支/站位/标签/特性
    for line in (fp.parse_charinfo(wt) or []):
        if "：" not in line:
            continue
        k, v = line.split("：", 1)
        k, v = k.strip(), v.strip()
        if k == "星级":
            try:
                row["star"] = int(v)
            except ValueError:
                row["star"] = None
        elif k == "名称":
            continue                       # 名字由调用方传
        else:
            row[{"职业": "profession", "分支": "branch", "站位": "position",
                 "标签": "tags_raw", "特性": "trait"}.get(k, k)] = v or None

    # 2) 档案的「基础档案」小节：种族/性别/出身地/战斗经验/生日/身高
    arch = D.parse_archive(_sections(wt).get("干员档案", "")) or []
    found = {}
    for head, text in arch:
        if "基础档案" in head:
            found = dict(ARCHIVE_FIELD_RE.findall(text))
            break
    row["race"] = (found.get("种族") or "").strip() or None
    row["gender"] = (found.get("性别") or "").strip() or None
    row["origin"] = (found.get("出身地") or "").strip() or None
    row["combat_exp"] = (found.get("战斗经验") or "").strip() or None
    row["birthday"] = (found.get("生日") or "").strip() or None
    row["height"] = (found.get("身高") or "").strip() or None

    # 3) 属性段表头：攻击间隔 / 再部署
    lines = _attr_lines(wt)
    head = lines[0] if lines else ""
    m = re.search(r"攻击速度\s*(\S+)", head)
    row["attack_interval"] = m.group(1) if m else None
    m = re.search(r"再部署\s*(\S+)", head)
    row["redeploy"] = m.group(1) if m else None

    # 4) 信赖加成（缺失填 0：源模板里没写就是 0）
    trust = {"生命": 0, "攻击": 0, "防御": 0}
    for ln in lines:
        if ln.startswith("信赖加成"):
            for k, v in re.findall(r"(生命|攻击|防御)\s*\+(\d+)", ln):
                trust[k] = int(v)
            break
    row["trust_hp"] = trust["生命"]
    row["trust_atk"] = trust["攻击"]
    row["trust_def"] = trust["防御"]

    # 5) 精英等级上限（3 星及以下没有 E1/E2 -> None）
    for ph in ("0", "1", "2"):
        m = re.search(r"精英" + ph + r" 满级（(\d+)级）", "\n".join(lines))
        row["level_cap_e" + ph] = int(m.group(1)) if m else None

    return row


def parse_tags(wt):
    """dim_tag 需要的标签列表。"""
    for line in (fp.parse_charinfo(wt) or []):
        if line.startswith("标签："):
            return [t for t in line.split("：", 1)[1].split() if t]
    return []


# ---------- dim_faction ----------

def parse_factions(wt):
    """返回 [(faction_name, relation, faction_level), ...]

    relation = '所属' / '隐藏'
    所属势力：逗号分隔=层级，level 是位置（1,2）
    隐藏势力：一般 ;; 分隔=并列（level 按逗号位置），这里统一按"组内位置"给 level；
             但为了和已入库的数据保持一致，隐藏一律给 NULL（见 03 脚本的说明）。
    """
    owned, hidden = fp.parse_faction(wt)
    out = []
    for idx, f in enumerate(owned, start=1):
        out.append((f, "所属", idx))
    for f in hidden:
        out.append((f, "隐藏", None))
    return out


# ---------- dim_module ----------

def parse_modules(wt):
    """返回 [(module_name, {列名: 值}), ...]"""
    lines = _attr_lines(wt)
    line = next((l for l in lines if l.startswith("模组：")), None)
    if not line:
        return []

    out = []
    for chunk in re.split(r"；", line.split("：", 1)[1]):
        chunk = chunk.strip()
        if not chunk:
            continue
        m = MODULE_RE.match(chunk)
        if not m:
            continue
        mod_name, bonus = m.group(1).strip(), m.group(2)
        cols = {}
        for kv in bonus.split(";"):
            kv = kv.strip()
            if not kv:
                continue
            parts = re.split(r"[:：]", kv, maxsplit=1)
            if len(parts) != 2:
                continue
            col = MODULE_KEY_MAP.get(parts[0].strip())
            if col is None:
                continue
            try:
                cols[col] = int(parts[1].strip())
            except ValueError:
                pass
        out.append((mod_name, cols))
    return out


# ---------- dim_potential ----------

def _parse_potential_desc(desc):
    if any(k in desc for k in POTENTIAL_SPECIAL):
        return None, None, True
    for prefix, btype in POTENTIAL_TYPE_MAP:
        if desc.startswith(prefix):
            m = NUM_RE.search(desc)
            if not m:
                return btype, None, False
            n = int(m.group(2))
            return btype, (n if m.group(1) == "+" else -n), False
    return None, None, False


def parse_potentials(wt):
    """返回 [(potential_level, bonus_type, bonus_value, is_special), ...]

    注意：不要自己从原始章节里抠。fp.parse_potential() 已经把
    {{潜能提升|...}} 模板翻译成 '潜能2：部署费用-1' 这种可读文本了。
    直接用它，再按句式分类。
    """
    body = _sections(wt).get("潜能提升", "")
    out = []
    for line in (fp.parse_potential(body) or []):
        m = POTENTIAL_LINE_RE.match(line.strip())
        if not m:
            continue
        lvl = int(m.group(1))
        btype, bval, special = _parse_potential_desc(m.group(2).strip())
        out.append((lvl, btype, bval, special))
    return out


# ---------- fact_operator_attr ----------

def _expand_chain(raw, n_elite, field):
    """'19→21→23' -> {0:.., 1:.., 2:..}（按精英等级索引）"""
    if not raw:
        return {}
    parts = [p.strip() for p in raw.replace("，", ",").split("→") if p.strip()]
    if not parts:
        return {}
    if len(parts) == 1:
        return {0: parts[0], 1: parts[0], 2: parts[0]}
    if len(parts) == n_elite:
        return {i: parts[i] for i in range(len(parts))}
    if field == "cost" and n_elite == 3 and len(parts) == 2:
        return {0: parts[0], 1: parts[1], 2: parts[1]}     # E1=E2 假设
    return {i: parts[min(i, len(parts) - 1)] for i in range(3)}


def _to_int(v):
    try:
        return int(str(v).strip())
    except (TypeError, ValueError):
        return None


def _parse_stats(text):
    out = {}
    for key, col in (("生命", "hp"), ("攻击", "atk"), ("防御", "def"), ("法抗", "res")):
        m = re.search(key + r"\s*(\d+)", text)
        out[col] = int(m.group(1)) if m else None
    return out


def parse_attributes(wt):
    """返回 fact_operator_attr 的行列表（不含 operator_name）。"""
    lines = _attr_lines(wt)
    joined = "\n".join(lines)

    phases, max_elite = [], 0
    for line in lines:
        m = PHASE_LINE_RE.match(line)
        if not m:
            continue
        elite, grade = int(m.group(1)), m.group(2)
        pid = PHASE_ID.get((elite, grade))
        if pid is None:
            continue
        phases.append((pid, _parse_stats(m.group(3)), elite))
        max_elite = max(max_elite, elite)

    n_elite = max_elite + 1
    mb = re.search(r"阻挡数\s*(\S+)", joined)
    mc = re.search(r"部署费用\s*(\S+)", joined)
    block_map = _expand_chain(mb.group(1) if mb else None, n_elite, "block")
    cost_map = _expand_chain(mc.group(1) if mc else None, n_elite, "cost")

    out = []
    for pid, st, elite in phases:
        out.append({
            "phase_id": pid,
            "hp": st.get("hp"), "atk": st.get("atk"),
            "def": st.get("def"), "res": st.get("res"),
            "block": _to_int(block_map.get(elite)),
            "cost": _to_int(cost_map.get(elite)),
        })
    return out


if __name__ == "__main__":
    # 自测：直接抓一个干员的 wikitext，把所有解析结果打出来
    import io
    import json
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

    for name in ["凯尔希", "星熊", "巡林者"]:
        wt = fp.get_wikitext(name)
        print(f"===== {name} =====")
        print("operator:", json.dumps(parse_operator(wt), ensure_ascii=False))
        print("tags    :", parse_tags(wt))
        print("factions:", parse_factions(wt))
        print("modules :", parse_modules(wt))
        print("potential:", parse_potentials(wt))
        print("attr    :", json.dumps(parse_attributes(wt), ensure_ascii=False))
        print()
