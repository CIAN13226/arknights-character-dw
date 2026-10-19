--- ==================== 维度表 ====================

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
  bonus_interval     INT            COMMENT '攻击间隔改动'
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