-- cur_month: 当前月的第一天

INSERT INTO `event_ros` PARTITION (`year`, `month`, `day`)
SELECT  `url`,
        `cms_type`,
        `cms_id`,
        `traffic`,
        `ip`,
        `time`,
        CAST(YEAR(`time`) AS SMALLINT),
        CAST(MONTH(`time`) AS SMALLINT),
        CAST(DAY(`time`) AS SMALLINT)
FROM `event_wos`
WHERE time >= MONTHS_SUB("${var:cur_month}", 1)
AND time < "${var:cur_month}";

COMPUTE INCREMENTAL STATS `event_ros`;