# arknights_dw · 明日方舟干员数据仓库

用 PRTS 的干员数据，按维度建模的方式搭一个分层数据仓库。
数据源是同一个仓库里的 RAG 项目（`../rag_project`），这里不重新抓取，只做建模和分层。

**当前进度：模型设计完成，7 张表的 DDL 已建，`dim_phase` 已灌。其余表待灌数据。**

---

## 这是什么

一个练习用的数据仓库，目标是把"一堆 JSON 和 Markdown"变成"能按维度切、能算指标"的结构化数据。

数据规模：

| 项 | 数值 |
|:---|---:|
| 干员 | 463 |
| 干员属性快照（事实行） | 1,796 |
| 势力归属记录 | 661 |
| 模组记录 | 513 |
| 标签记录 | 819 |
| 潜能加成记录 | 约 2,300 |

---

## 模型设计

**雪花模型**：1 张事实表 + 6 张维度表。

```
        ┌──────────────┐   ┌──────────────┐   ┌──────────────┐   ┌──────────────────┐
        │  dim_faction │   │  dim_module  │   │   dim_tag    │   │  dim_potential   │
        │     (N)      │   │     (N)      │   │     (N)      │   │       (N)        │
        └──────┬───────┘   └──────┬───────┘   └──────┬───────┘   └────────┬─────────┘
               │                  │                  │                    │
               └──────────────────┼──────────────────┼────────────────────┘
                                  │ 箭头指向 ↓
                       ┌──────────▼───────────┐
                       │    dim_operator      │
                       │        (1)           │
                       └──────────▲───────────┘
                                  │ 箭头指向 ↑
                       ┌──────────┴───────────┐
                       │  fact_operator_attr  │
                       │        (N)           │
                       └──────────┬───────────┘
                                  │ 箭头指向 ↓
                       ┌──────────▼───────────┐
                       │      dim_phase       │
                       │        (1)           │
                       └──────────────────────┘
```

### 表清单

| 表 | 类型 | 主键 | 一行代表 | 行数 |
|:---|:---|:---|:---|---:|
| `dim_operator` | 维度 | `operator_name` | 一个干员 | 463 |
| `dim_phase` | 维度 | `phase_id` | 一个精英阶段档位 | 4 |
| `dim_faction` | 维度（多值） | `(operator_name, faction_name, relation)` | 一个干员的一个势力归属 | 661 |
| `dim_module` | 维度（多值） | `(operator_name, module_name)` | 一个干员的一个模组 | 513 |
| `dim_tag` | 维度（多值） | `(operator_name, tag)` | 一个干员的一个标签 | 819 |
| `dim_potential` | 维度（多值） | `(operator_name, potential_level)` | 一个干员的一个潜能加成 | ~2300 |
| `fact_operator_attr` | **事实** | `(operator_name, phase_id)` | 一个干员的某阶段属性快照 | **1,796** |

### 为什么是雪花而不是星型

星型要求"每个维度表都直接描述事实"。势力、模组、标签描述的是**干员**，不是**某次属性快照**，所以它们只能连 `dim_operator`。一旦出现"维度表 → 维度表"的连线，就是雪花。

想做成纯星型只有两条路，都不划算：

| 路 | 做法 | 代价 |
|:---|:---|:---|
| 压成数组列 | 势力/模组/标签各存一个 `ARRAY<STRING>` | 失去精确筛选，"哪个势力人最多"要 explode 才能算 |
| 塞进事实表 | 事实表加 faction 列 | 粒度被污染：1,796 行涨到 5,000+，且 hp/atk 重复 |

### 几个建模决策

**① 维度 vs 事实的判据是"能不能求和/求平均"，不是"描述抽象还是描述动作"**

星级是数字但不能求和 → 维度表。部署费用能求平均 → 事实表。

**② 一个干员有多个值 → 拆成多行，不是加多列**

势力、模组、标签、潜能都是这个道理。用 `level1/level2` 两列装不下"凯尔希同时属于罗德岛和巴别塔"这种情况。

**③ 阻挡数和部署费用属于事实，不属于维度**

实测：**34 个干员**的阻挡数随精英阶段变（如百炼嘉维尔 `2→2→3`），
**446 个干员**的部署费用会涨（分布：1 段 17 个、2 段 343 个、3 段 103 个）。
其余属性（攻击间隔、再部署时间）全库都是单值，放维度表。

**④ 等级上限放维度表，不按固定规则**

上限随星级变，不是固定的 50/80/90：

| 干员 | 星级 | E0 | E1 | E2 |
|:---|:---|:---|:---|:---|
| 巡林者 | 2 | 30 | — | — |
| 芬 | 3 | 40 | 55 | — |
| 星熊 | 6 | 50 | 80 | 90 |

**⑤ 初始模组不进表**

`初始模组名`（如「星熊证章」）只有名字没有属性数据，是默认栏位。

---

## 运行

**环境**：Python 3.11 + PySpark 4.2 + JDK 21。

Windows 上有两个必踩的坑，先处理：

```powershell
# 坑 1：不设这个会报 HADOOP_HOME and hadoop.home.dir are unset
$env:HADOOP_HOME = "E:\hadoop"
$env:PATH = "E:\hadoop\bin;$env:PATH"
```

```text
坑 2：脚本不能放在中文目录下。
在中文路径下跑 createDataFrame 会报
  SparkException: Python worker failed to connect back
因为中文路径名在启动 Python worker 时被编码破坏。项目目录必须是纯 ASCII。
```

**建表：**

```powershell
cd arknights_dw
python scripts\01_create_tables.py
```

预期输出：7 张表 + `dim_phase` 4 行。

---

## 项目结构

```text
arknights_dw/
├── sql/
│   └── ddl.sql                 7 张表的建表语句
├── scripts/
│   └── 01_create_tables.py     建库 + 建表 + 灌 dim_phase
├── spark-warehouse/            表数据（parquet，不提交）
├── metastore_db/               Derby 元数据（不提交）
├── derby.log                   不提交
└── .gitignore
```

**仓库里只有手写的文件**：DDL、脚本、README。所有数据都能用脚本重建。

---

## 进度

- [x] 模型设计（7 张表，雪花结构）
- [x] DDL 编写并实测（Spark 4.2 跑通，7/7）
- [x] **ODS 层：`ods_prts_wikitext`（463 行，592 万字符原始 wiki 源码）**
- [x] `dim_phase`（4 行）
- [x] `dim_operator`（463 行）—— 解析 `operators.json` + `corpus_db` 属性段
- [x] `dim_faction`（668 行）—— 所属 545 + 隐藏 123
- [x] `dim_tag`（819 行）
- [x] `fact_operator_attr`（1,796 行）—— 阶段分布 463/463/446/424
- [x] `dim_module`（513 行）—— 模组加成拆成 8 个数值列
- [x] `dim_potential`（2,155 行）—— 431 干员 × 5 潜能
- [ ] **把 5 个 DWD 脚本改成只从 ODS 读**（当前仍直接读源文件，属于跨层）
- [ ] DWS 汇总层（存 sum/cnt，不存平均）
- [ ] ADS 应用层
- [ ] 数据质量校验脚本（行数对账、主键唯一性、跨表一致性）
- [ ] 调度（DolphinScheduler 或 Airflow 串起来）

**当前 8 张表合计 6,881 行数据。**

### 分层现状（诚实说明）

| 层 | 状态 |
|:---|:---|
| ODS | ✅ `ods_prts_wikitext`，463 行原始 wikitext |
| DWD | ✅ 7 张表已建好，但**数据来自源文件，还没改成读 ODS** |
| DWS | ❌ 未做 |
| ADS | ❌ 未做 |

**ODS→DWD 的通路已验证可行**（从 ODS 取 wikitext 跑解析，结果与现有 DWD 完全一致），
下一步就是把 5 个加载脚本的数据来源换掉。

---

## 示例查询

### 1. 跨三张表：各一级势力的六星人数与精英2平均攻击

```sql
WITH fac AS (
  SELECT operator_name, faction_name FROM dim_faction
  WHERE relation='所属' AND faction_level=1        -- 只取一级势力，避免和二级重复计数
),
six AS (SELECT operator_name FROM dim_operator WHERE star=6),
attr AS (SELECT operator_name, atk FROM fact_operator_attr WHERE phase_id=4)
SELECT fac.faction_name, COUNT(*), ROUND(AVG(attr.atk),1)
FROM fac JOIN six ON fac.operator_name=six.operator_name
         JOIN attr ON fac.operator_name=attr.operator_name
GROUP BY fac.faction_name HAVING COUNT(*) >= 3
ORDER BY COUNT(*) DESC;
```

实测结果（部分）：

| 势力 | 六星人数 | 平均攻击 |
|:---|---:|---:|
| 罗德岛 | 26 | 661.8 |
| 哥伦比亚 | 13 | 613.4 |
| 炎 | 11 | 625.5 |
| 维多利亚 | 11 | 583.0 |
| 乌萨斯 | 4 | **1116.5** |
| 巴别塔 | 4 | 941.0 |

### 2. ⚠️ 一个必须知道的陷阱：join 多值维度会放大指标

**这是数仓里最常见、也最不报错的一类 bug。**

想算「罗德岛六星干员的模组加攻总和」：

```sql
-- 正确：只 join 必要的表
WITH rop AS (SELECT DISTINCT operator_name FROM dim_faction
             WHERE relation='所属' AND faction_name='罗德岛')
SELECT COUNT(*), SUM(m.bonus_atk)
FROM dim_module m
JOIN rop ON m.operator_name = rop.operator_name
JOIN dim_operator o ON m.operator_name = o.operator_name
WHERE o.star=6 AND m.bonus_atk IS NOT NULL;
-- 结果：43 条，加攻总和 2989
```

**如果再多 join 一个多值维度 `dim_tag`（看起来无害，只是想顺便带个标签）：**

```sql
... JOIN dim_tag t ON m.operator_name = t.operator_name
-- 结果：93 条，加攻总和 6418   ← 行数和总量都被放大了 2.15 倍
```

**原因**：一个干员有多个标签，join 后每个模组行被复制了「标签数」次。
`COUNT` 和 `SUM` 全部翻倍，而且**不报错、结果看着也像正常数字**。

**规避办法**：一张事实表/多值维度，**不要在同一个查询里同时 join 多个 1:N 的多值维度**。
需要多个维度的信息时，先分别聚合到干员粒度，再在干员粒度上 join。

---

## 已知限制

---

## 已知限制

- **部署费用"2 段"的 E1 值有歧义。** 源数据只给了链式值（凯尔希 `18→20`），
  无法确定 E1 是 18 还是 20。当前按"E1=E2"填充，这个假设写在 DDL 注释里。
  343 个干员受影响。
- **潜能的天赋类加成无数值。** 源数据里这类位置是空字符串（表示"特殊效果"），
  表里用 `is_special = true` 标记，具体效果没有存。
- **只覆盖明日方舟一款游戏。** 换游戏需要重写抓取和解析。
- **没有做缓慢变化维（SCD）。** 当前数据是一次性快照，没有历史版本。

---

## 数据来源

数据抓取自 [PRTS.wiki](https://prts.wiki/)。一切涉及的数据内容版权归上海鹰角网络科技有限公司所有。
本项目仅供学习研究，不用于商业用途。

## 相关

- 抓取与解析代码、RAG 问答系统：[`../rag_project`](../rag_project)
- 完整建模过程（含踩过的 13 个坑）：`../学习规划/PRTS数仓模型.md`

## License

MIT
