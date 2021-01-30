-- cur_month: 当前月的第一天

ALTER TABLE `event_wos`
DROP IF EXISTS RANGE
PARTITION CAST(MONTHS_SUB("${var:cur_month}", 1) AS STRING) <= VALUES < "${var:cur_month}";

ALTER TABLE `event_wos`
ADD IF NOT EXISTS RANGE
PARTITION CAST(MONTHS_ADD("${var:cur_month}", 1) AS STRING) <= VALUES < CAST(MONTHS_ADD("${var:cur_month}", 2) AS STRING);