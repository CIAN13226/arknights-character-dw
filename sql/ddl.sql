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