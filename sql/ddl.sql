-- ==================== ODS 层 ====================
-- 原则：原样落地，不做任何业务加工。
-- 这里存的是 PRTS 页面的 wikitext 原文（约 8MB / 463 条）。
-- 下游 DWD 层只从这张表读，不再碰任何文件，
-- 这样数仓从 wiki 原始源码开始，不依赖别的项目的中间产物。

CREATE TABLE IF NOT EXISTS ods_prts_wikitext (
  operator_name  STRING NOT NULL COMMENT '干员名，同时也是 PRTS 页面标题',
  wikitext       STRING         COMMENT '页面 wikitext 原文，未做任何加工（含换行，不截断）',
  char_len       INT            COMMENT '字符数，用于校验入库无截断',
  etl_date       STRING         COMMENT '抽取日期 yyyy-MM-dd',
  source_url     STRING         COMMENT '来源 URL'
) USING PARQUET;

-- ==================== 维度表 ====================

CREATE TABLE IF NOT EXISTS dim_operator (
  operator_name    STRING NOT NULL COMMENT '干员代号（PK）',
  star             INT            COMMENT '星级 1-6',
  profession       STRING         COMMENT '职业',
  branch           STRING         COMMENT '分支',
  position         STRING         COMMENT '站位',
  race             STRING         COMMENT '种族',
  gender           STRING         COMMENT '性别',
  origin           STRING         COMMENT '出身地',
  birthday         STRING         COMMENT '生日',
  height           STRING         COMMENT '身高，形如 169cm',
  combat_exp       STRING         COMMENT '战斗经验',
  trait            STRING         COMMENT '特性',
  attack_interval  STRING         COMMENT '基础攻击间隔，形如 1.2s',
  redeploy         STRING         COMMENT '再部署时间，形如 70s',
  trust_hp         INT            COMMENT '信赖加成-生命',
  trust_atk        INT            COMMENT '信赖加成-攻击',
  trust_def        INT            COMMENT '信赖加成-防御',
  level_cap_e0     INT            COMMENT '精英0 等级上限',
  level_cap_e1     INT            COMMENT '精英1 等级上限',
  level_cap_e2     INT            COMMENT '精英2 等级上限'
) USING PARQUET;

CREATE TABLE IF NOT EXISTS dim_phase (
  phase_id     INT    NOT NULL COMMENT '1-4（PK）',
  phase        STRING         COMMENT '精英0/精英1/精英2',
  level_grade  STRING         COMMENT '1级/满级'
) USING PARQUET;

CREATE TABLE IF NOT EXISTS dim_faction (
  operator_name  STRING NOT NULL,
  faction_name   STRING NOT NULL COMMENT '势力名',
  relation       STRING NOT NULL COMMENT '所属/隐藏',
  faction_level  INT            COMMENT '1=一级 2=二级'
) USING PARQUET;

CREATE TABLE IF NOT EXISTS dim_module (
  operator_name      STRING NOT NULL,
  module_name        STRING NOT NULL,
  bonus_atk          INT,
  bonus_def          INT,
  bonus_hp           INT,
  bonus_res          INT,
  bonus_block        INT,
  bonus_attack_speed INT            COMMENT '攻速加成 S',
  bonus_cost         INT            COMMENT '部署费用加成',
  bonus_respawn_time INT            COMMENT '再部署时间加成'
) USING PARQUET;

CREATE TABLE IF NOT EXISTS dim_tag (
  operator_name  STRING NOT NULL,
  tag            STRING NOT NULL
) USING PARQUET;

CREATE TABLE IF NOT EXISTS dim_potential (
  operator_name    STRING NOT NULL,
  potential_level  INT    NOT NULL COMMENT '2-6',
  bonus_type       STRING         COMMENT 'cost/re_deploy/def/atk/interval',
  bonus_value      INT            COMMENT '数值，可为负',
  is_special       BOOLEAN        COMMENT '天赋类特殊效果（无数值）'
) USING PARQUET;

-- ==================== 事实表 ====================

CREATE TABLE IF NOT EXISTS fact_operator_attr (
  operator_name  STRING NOT NULL COMMENT 'FK → dim_operator',
  phase_id       INT    NOT NULL COMMENT 'FK → dim_phase',
  hp             INT            COMMENT '生命上限',
  atk            INT            COMMENT '攻击',
  def            INT            COMMENT '防御',
  res            INT            COMMENT '法术抗性',
  block          INT            COMMENT '阻挡数',
  cost           INT            COMMENT '部署费用'
) USING PARQUET;

-- ==================== DWS 层 ====================
-- 铁律：只存「可加度量」（sum / count），**不存平均值、不存比率**。
--
-- 为什么：平均值不可再聚合。
--   假设 DWS 里按星级存了平均攻击：五星 600、六星 700。
--   想算整体的平均攻击，写成 (600+700)/2 = 650 是错的。
--   正确值是加权平均：如果五星 300 人、六星 26 人，
--   应该是 (600*300 + 700*26) / 326。
--   除非同时知道每组的人数，否则无法还原。
-- 所以这里存 atk_sum + atk_cnt，平均 = SUM(atk_sum)/SUM(atk_cnt)，任意层级都算得对。
--
-- 另一个必须知道的点：势力是多值维度（一个干员可属多个势力），
-- 所以「跨势力求和」会重复计数（同一干员在多个势力里各算一次）。
-- 这是多值维度的固有特性，看汇总数字时要注意口径。

CREATE TABLE IF NOT EXISTS dws_faction_star (
  faction_name   STRING NOT NULL COMMENT '一级势力名（来自 dim_faction 层级1）',
  star           INT    NOT NULL COMMENT '星级',
  phase_id       INT    NOT NULL COMMENT '精英阶段档位（来自 dim_phase）',
  operator_cnt   INT            COMMENT '该组合下干员数（去重）',
  atk_sum        INT            COMMENT '攻击合计',
  atk_cnt        INT            COMMENT '攻击非空计数（算平均的分母）',
  hp_sum         INT            COMMENT '生命合计',
  hp_cnt         INT            COMMENT '生命非空计数',
  cost_sum       INT            COMMENT '部署费用合计',
  cost_cnt       INT            COMMENT '费用非空计数'
) USING PARQUET;

CREATE TABLE IF NOT EXISTS dws_profession_branch (
  profession     STRING NOT NULL COMMENT '职业',
  branch         STRING NOT NULL COMMENT '分支',
  phase_id       INT    NOT NULL COMMENT '精英阶段档位',
  operator_cnt   INT            COMMENT '该组合下干员数',
  atk_sum        INT,
  atk_cnt        INT,
  hp_sum         INT,
  hp_cnt         INT,
  def_sum        INT,
  def_cnt        INT,
  cost_sum       INT,
  cost_cnt       INT
) USING PARQUET;

CREATE TABLE IF NOT EXISTS dws_tag (
  tag            STRING NOT NULL COMMENT '标签',
  operator_cnt   INT            COMMENT '带该标签的干员数',
  six_star_cnt   INT            COMMENT '其中六星人数',
  star_sum       INT            COMMENT '星级合计',
  star_cnt       INT            COMMENT '星级非空计数'
) USING PARQUET;

CREATE TABLE IF NOT EXISTS dws_module_effect (
  profession          STRING NOT NULL COMMENT '职业',
  module_cnt          INT            COMMENT '模组条数',
  operator_cnt        INT            COMMENT '有模组的干员数',
  bonus_atk_sum       INT            COMMENT '模组加攻合计',
  bonus_atk_cnt       INT            COMMENT '有加攻的模组条数',
  bonus_hp_sum        INT,
  bonus_hp_cnt        INT,
  bonus_def_sum       INT,
  bonus_def_cnt       INT,
  bonus_attack_speed_sum INT,
  bonus_attack_speed_cnt INT
) USING PARQUET;

-- 数据完整度：给数据治理用的，不是业务指标。
-- 一行一个干员，记录 dim_operator 的 12 个描述字段里缺了几个，
-- 以及几个关键的多值维度有没有数据。
CREATE TABLE IF NOT EXISTS dws_operator_completeness (
  operator_name   STRING NOT NULL,
  star            INT,
  profession      STRING,
  missing_cnt     INT            COMMENT '12 个描述字段中缺失的个数',
  total_cnt       INT            COMMENT '参与检查的字段总数',
  has_faction     INT            COMMENT '1=有, 0=无',
  has_tag         INT,
  has_module      INT,
  has_potential   INT,
  has_trust       INT            COMMENT '是否有信赖加成（三项任一非0）'
) USING PARQUET;
-- ==================== ADS 层 ====================
-- 和 DWS 最关键的区别：**ADS 可以存平均值和排名**。
--
-- 为什么 DWS 不能存平均、ADS 却能：
--   DWS 的下游还要再聚合（可能按势力汇总、可能跨星级汇总），
--   平均值一旦存死就无法还原；而 ADS 是链路的终点，
--   它直接喂报表/看板/接口，没有下游再算一次，所以存成品值是对的。
--   换句话说：**不可加度量只能在"最后一层"出现。**
--
-- ADS 只从 DWS 读，不再回 DWD 取数。

-- 势力排行榜：给看板用的成品表（精英2满级口径）
CREATE TABLE IF NOT EXISTS ads_faction_rank (
  faction_name    STRING NOT NULL COMMENT '一级势力',
  rank_no         INT            COMMENT '按人数排名（1 起）',
  operator_cnt    INT            COMMENT '干员数',
  six_star_cnt    INT            COMMENT '六星人数',
  avg_atk         DOUBLE         COMMENT '平均攻击（成品值）',
  avg_hp          DOUBLE         COMMENT '平均生命',
  avg_cost        DOUBLE         COMMENT '平均部署费用'
) USING PARQUET;

-- 职业画像：给看板的成品表（精英2满级口径）
CREATE TABLE IF NOT EXISTS ads_profession_profile (
  profession      STRING NOT NULL COMMENT '职业',
  operator_cnt    INT            COMMENT '干员数',
  branch_cnt      INT            COMMENT '下设分支数',
  avg_atk         DOUBLE,
  avg_hp          DOUBLE,
  avg_def         DOUBLE,
  avg_cost        DOUBLE
) USING PARQUET;

-- 数据质量总览：给监控看板的单表宽指标（长表结构，便于前端直接渲染）
CREATE TABLE IF NOT EXISTS ads_data_quality_summary (
  metric_name     STRING NOT NULL COMMENT '指标名',
  metric_value    DOUBLE         COMMENT '指标值',
  metric_note     STRING         COMMENT '口径说明'
) USING PARQUET;
