package com.wzx.common

object FilePath {
  val YEAR_PATTERN = "{YEAR}"
  val MONTH_PATTERN = "{MONTH}"
  val DAY_PATTERN = "{DAY}"

  val STATE_BACKEND_PATH = "hdfs://master:8020/user/wzx/flink_state_backend"
  val EVENT_ROS_PARQUET = s"hdfs://master:8020/user/wzx/hive/${TableName.EVENT_ROS}/{YEAR}/{MONTH}/{DAY}"
  val USER_TAG_1_ROS_PARQUET = s"hdfs://master:8020/user/wzx/hive/${TableName.USER_TAG_1_ROS}/{YEAR}/{MONTH}/{DAY}"
  val USER_TAG_2_ROS_PARQUET = s"hdfs://master:8020/user/wzx/hive/${TableName.USER_TAG_2_ROS}/{YEAR}/{MONTH}/{DAY}"
}
