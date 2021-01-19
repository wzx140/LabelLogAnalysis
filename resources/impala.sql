CREATE TABLE `event_wos`
(
    `ip`       STRING COMMENT '用户ip',
    `year`     SMALLINT COMMENT 'yyyy',
    `month`    SMALLINT COMMENT 'MM',
    `day`      SMALLINT COMMENT 'dd',
    `time`     STRING COMMENT 'yyyy-MM-dd HH:mm:ss',
    `url`      STRING COMMENT '访问url',
    `cms_type` STRING COMMENT '访问类型',
    `cms_id`   BIGINT COMMENT '访问类型对应的编号',
    `traffic`  BIGINT COMMENT '流量',
    PRIMARY KEY (`ip`, `year`, `month`, `day`, `time`)
) PARTITION BY
   HASH(`ip`) PARTITIONS 2,
   RANGE(`month`) (
      PARTITION 0 < VALUES <= 1, --一月
      PARTITION 1 < VALUES <= 2, --二月
      PARTITION 2 < VALUES <= 3, --三月
      PARTITION 3 < VALUES <= 4, --四月
      PARTITION 4 < VALUES <= 5, --五月
      PARTITION 5 < VALUES <= 6, --六月
      PARTITION 6 < VALUES <= 7, --七月
      PARTITION 7 < VALUES <= 8, --八月
      PARTITION 8 < VALUES <= 9, --九月
      PARTITION 9 < VALUES <= 10, --十月
      PARTITION 10 < VALUES <= 11, --十一月
      PARTITION 11 < VALUES <= 12 --十二月
)
STORED AS KUDU;

CREATE TABLE `profile_wos`
(
    `ip`           STRING COMMENT '用户ip',
    `city`         STRING COMMENT '用户城市',
    `register_day` STRING COMMENT '注册日期',
    PRIMARY KEY (`ip`)
) PARTITION BY
   HASH(`ip`) PARTITIONS 2
STORED AS KUDU;

CREATE TABLE `event_ros`
(
    `url`      STRING COMMENT '访问url',
    `cms_type` STRING COMMENT '访问类型',
    `cms_id`   BIGINT COMMENT '访问类型对应的编号',
    `traffic`  BIGINT COMMENT '流量',
    `ip`       STRING COMMENT '用户ip',
    `time`     STRING COMMENT 'yyyy-MM-dd HH:mm:ss'
) PARTITIONED BY(
    `year`     SMALLINT COMMENT 'yyyy',
    `month`    SMALLINT COMMENT 'MM',
    `day`      SMALLINT COMMENT 'dd')
STORED AS PARQUET;

CREATE TABLE `user_tag_ros`
(
    `ip`             STRING COMMENT '用户ip',
    `week_register`  BOOLEAN COMMENT '近一周新注册',
    `video_over_100` BOOLEAN COMMENT '当天video访问量超过100'
) PARTITION BY(
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
       `time`,
       `year`,
       `month`,
       `day`
FROM `event_wos`
WHERE `time` >= TRUNC(ADD_MONTHS(NOW(), -1), 'MM')
UNION ALL
SELECT `url`,
       `cms_type`,
       `cms_id`,
       `traffic`,
       `ip`,
       `time`,
       `year`,
       `month`,
       `day`
FROM `event_ros`
WHERE `time` < TRUNC(ADD_MONTHS(NOW(), -1), 'MM');

CREATE VIEW `event_external_view` AS
SELECT `event_view`.`url`,
       `event_view`.`cms_type`,
       `event_view`.`cms_id`,
       `event_view`.`traffic`,
       `event_view`.`ip`,
       `profile_wos`.`city`,
       `profile_wos`.`register_day`,
       `event_view`.`time`,
       `event_view`.`year`,
       `event_view`.`month`,
       `event_view`.`day`
FROM `event_view`
         join /* +BROADCAST */ `profile_wos` on `event_view`.ip = `profile_wos`.ip;
