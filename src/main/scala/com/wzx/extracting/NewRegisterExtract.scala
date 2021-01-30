package com.wzx.extracting

import com.typesafe.config.ConfigFactory
import com.wzx.common.{FilePath, TableName}
import com.wzx.entity.Profile
import com.wzx.util.{DateUtil, OptionUtil}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import java.time.LocalDateTime

/**
  * 最近一周新注册用户
  */
object NewRegisterExtract {

  private val name = this.getClass.getName.stripSuffix("$")
  private val log = LoggerFactory.getLogger(name)
  private val config = ConfigFactory.load("application.conf")
  private val spark = SparkSession
    .builder()
    .appName(name)
    .getOrCreate()

  def extract(
      profileDS: Dataset[Profile],
      date: LocalDateTime
  ): Dataset[String] = {
    val beforeDay = DateUtil.formatDay(date.minusWeeks(1))
    val afterDay = DateUtil.formatDay(date)
    import spark.implicits._
    profileDS
      .filter($"register_day" > beforeDay && $"register_day" <= afterDay)
      .select($"ip")
      .as[String]
  }

  def main(args: Array[String]): Unit = {
    val (date, beforeDate) = OptionUtil.getTime(args)
    log.info(DateUtil.formatLine(beforeDate))

    // 读取kudu中的数据
    import spark.implicits._
    val profileDS = spark.read
      .option("kudu.master", config.getString("wzx.db.kudu.master_url"))
      .option("kudu.table", TableName.PROFILE_WOS)
      .format("kudu")
      .load()
      .as[Profile]

    val outDS = extract(profileDS, beforeDate)
    // 写入parquet
    val outputPath = DateUtil
      .formatDateString(FilePath.USER_TAG_2_ROS_PARQUET, date)
    outDS.write
      .mode(SaveMode.Overwrite)
      .parquet(
        outputPath
      )

    spark.close()
  }
}
