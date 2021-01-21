package com.wzx.extracting

import com.typesafe.config.ConfigFactory
import com.wzx.common.{FilePath, TableName}
import com.wzx.entity.Profile
import com.wzx.util.{DateUtil, OptionUtil}
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.{Dataset, Row, SparkSession}
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
    val kuduContext = new KuduContext(
      config.getString("wzx.db.kudu.master_url"),
      spark.sqlContext.sparkContext
    )
    import spark.implicits._
    val profileDS = kuduContext
      .kuduRDD(spark.sparkContext, TableName.PROFILE_WOS)
      .map {
        case Row(ip: String, city: String, register_day: String) =>
          Profile(ip, city, register_day)
      }
      .toDS()

    val outDS = extract(profileDS, beforeDate)
    // 写入parquet
    outDS.write.parquet(
      DateUtil
        .formatDateString(FilePath.USER_TAG_2_ROS_PARQUET, date)
    )

    spark.close()
  }
}
