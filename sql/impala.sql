-- cur_month: 当前月的第一天 yyyy-MM-dd HH:mm:ss

DROP TABLE IF EXISTS `event_wos`;
DROP TABLE IF EXISTS `profile_wos`;
DROP TABLE IF EXISTS `event_ros`;
DROP TABLE IF EXISTS `user_tag_1_ros`;
DROP TABLE IF EXISTS `user_tag_2_ros`;
DROP VIEW IF EXISTS `event_view`;
DROP VIEW IF EXISTS `event_external_view`;

CREATE TABLE `event_wos`
(
    `ip`       STRING COMMENT '用户ip',
    `time`     STRING COMMENT 'yyyy-MM-dd HH:mm:ss',
    `url`      STRING COMMENT '访问url',
    `cms_type` STRING COMMENT '访问类型',
    `cms_id`   INT COMMENT '访问类型对应的编号',
    `traffic`  INT COMMENT '流量',
    PRIMARY KEY (`ip`, `time`)
)
PARTITION BY
    HASH(`ip`) PARTITIONS 2,
RANGE(`time`) (
    PARTITION "${var:cur_month}" <= VALUES < CAST(MONTHS_ADD("${var:cur_month}", 1) AS STRING),
    -- 缓冲区
    PARTITION CAST(MONTHS_ADD("${var:cur_month}", 1) AS STRING) <= VALUES < CAST(MONTHS_ADD("${var:cur_month}", 2) AS STRING)
)
STORED AS KUDU;

CREATE TABLE `profile_wos`
(
    `ip`           STRING COMMENT '用户ip',
    `city`         STRING COMMENT '用户城市',
    `register_day` STRING COMMENT '注册日期',
    PRIMARY KEY (`ip`)
)
PARTITION BY
   HASH(`ip`) PARTITIONS 2
STORED AS KUDU;

CREATE TABLE `event_ros`
(
    `url`      STRING COMMENT '访问url',
    `cms_type` STRING COMMENT '访问类型',
    `cms_id`   INT COMMENT '访问类型对应的编号',
    `traffic`  INT COMMENT '流量',
    `ip`       STRING COMMENT '用户ip',
    `time`     STRING COMMENT 'yyyy-MM-dd HH:mm:ss'
)
PARTITIONED BY(
    `year`     SMALLINT COMMENT 'yyyy',
    `month`    SMALLINT COMMENT 'MM',
    `day`      SMALLINT COMMENT 'dd')
STORED AS PARQUET;

--近一周新注册
CREATE TABLE `user_tag_1_ros`
(
    `ip` STRING COMMENT '用户ip'
)
PARTITIONED BY(
    `year`     SMALLINT COMMENT 'yyyy',
    `month`    SMALLINT COMMENT 'MM',
    `day`      SMALLINT COMMENT 'dd')
STORED AS PARQUET;

--当天video访问量超过100
CREATE TABLE `user_tag_2_ros`
(
    `ip` STRING COMMENT '用户ip'
)
PARTITIONED BY(
    `year`     SMALLINT COMMENT 'yyyy',
    `month`    SMALLINT COMMENT 'MM',
    `day`      SMALLINT COMMENT 'dd')
STORED AS PARQUET;

CREATE VIEW `event_view` AS
SELECT `url`,
       `cms_type`,
       `cms_id`,
       `traffic`,
       `ip`,
       `time`
FROM `event_wos`
WHERE `time` >= "${var:cur_month}"
UNION ALL
SELECT `url`,
       `cms_type`,
       `cms_id`,
       `traffic`,
       `ip`,
       `time`
FROM `event_ros`
WHERE `time` < "${var:cur_month}"
AND `year` = YEAR (`time`)
AND `month` = MONTH (`time`)
AND `day` = DAY (`time`);

CREATE VIEW `event_external_view` AS
SELECT `event_view`.`url`,
       `event_view`.`cms_type`,
       `event_view`.`cms_id`,
       `event_view`.`traffic`,
       `event_view`.`ip`,
       `profile_wos`.`city`,
       `profile_wos`.`register_day`,
       `event_view`.`time`
FROM `event_view`
join /* +BROADCAST */ `profile_wos` on `event_view`.ip = `profile_wos`.ip;
