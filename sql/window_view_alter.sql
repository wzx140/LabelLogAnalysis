-- cur_month: 当前月的第一天

ALTER VIEW `event_view` AS
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
AND `year` = YEAR(`time`)
AND `month` = MONTH(`time`)
AND `day` = DAY(`time`);