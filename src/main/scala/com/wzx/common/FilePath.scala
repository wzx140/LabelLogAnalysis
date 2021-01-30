package com.wzx.common

object FilePath {
  val YEAR_PATTERN = """\{YEAR\}"""
  val MONTH_PATTERN = """\{MONTH\}"""
  val DAY_PATTERN = """\{DAY\}"""

  val STATE_BACKEND_PATH = "hdfs://master:8020/user/wzx/flink_state_backend"
  val EVENT_ROS_PARQUET = s"hdfs://master:8020/user/hive/warehouse/${TableName.EVENT_ROS}"
  val USER_TAG_1_ROS_PARQUET = s"hdfs://master:8020/user/hive/warehouse/${TableName.USER_TAG_1_ROS}/year={YEAR}/month={MONTH}/day={DAY}"
  val USER_TAG_2_ROS_PARQUET = s"hdfs://master:8020/user/hive/warehouse/${TableName.USER_TAG_2_ROS}/year={YEAR}/month={MONTH}/day={DAY}"
}
