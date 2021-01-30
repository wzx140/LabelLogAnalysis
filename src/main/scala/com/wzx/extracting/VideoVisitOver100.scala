package com.wzx.extracting

import com.typesafe.config.ConfigFactory
import com.wzx.common.{FilePath, TableName}
import com.wzx.entity.Event
import com.wzx.util.{DateUtil, OptionUtil}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

/**
  * 今年来video访问量超过100
  */
object VideoVisitOver100 {

  private val name = this.getClass.getName.stripSuffix("$")
  private val log = LoggerFactory.getLogger(name)
  private val config = ConfigFactory.load("application.conf")
  private val spark = SparkSession
    .builder()
    .appName(name)
    .getOrCreate()

  def extract(data: Dataset[Event]): Dataset[String] = {
    import spark.implicits._
    data
      .filter($"cms_type" === "video")
      .select($"ip", $"cms_type")
      .groupBy($"ip")
      .count()
      .filter($"count" >= 100)
      .select("ip")
      .as[String]
  }

  def main(args: Array[String]): Unit = {
    val (date, beforeDate) = OptionUtil.getTime(args)
    log.info(DateUtil.formatLine(beforeDate))

    // 读取parquet中的数据
    import spark.implicits._
    spark.conf.set("spark.sql.parquet.binaryAsString", "true")
    val eventParquetDS = spark.read
      .option("spark.sql.parquet.binaryAsString", "true")
      .parquet(FilePath.EVENT_ROS_PARQUET)
      .filter($"year" === date.getYear)
      .select($"ip", $"time", $"url", $"cms_type", $"cms_id", $"traffic")
      .as[Event]
    // 读取kudu中的数据
    val eventKuduDS = spark.read
      .option("kudu.master", config.getString("wzx.db.kudu.master_url"))
      .option("kudu.table", TableName.EVENT_WOS)
      .format("kudu")
      .load()
      .as[Event]
      .filter($"time" < DateUtil.formatLine(date))

    val outDS = extract(eventParquetDS.union(eventKuduDS))
    // 写入parquet
    val outputPath = DateUtil
      .formatDateString(FilePath.USER_TAG_1_ROS_PARQUET, date)
    outDS.write
      .mode(SaveMode.Overwrite)
      .parquet(
        outputPath
      )

    spark.close()
  }
}
