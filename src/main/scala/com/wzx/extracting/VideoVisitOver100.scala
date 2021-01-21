package com.wzx.extracting

import com.typesafe.config.ConfigFactory
import com.wzx.common.{FilePath, TableName}
import com.wzx.entity.Event
import com.wzx.util.{DateUtil, OptionUtil, SparkUtil}
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.{Dataset, SparkSession}
import org.slf4j.LoggerFactory

/**
  * 当日video访问量超过100
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
    val dataPath =
      DateUtil.formatDateString(FilePath.EVENT_ROS_PARQUET, beforeDate)
    val eventParquetDS = spark.read
      .parquet(dataPath)
      .as[Event]
    // 读取kudu中的数据
    val kuduContext = new KuduContext(
      config.getString("wzx.db.kudu.master_url"),
      spark.sqlContext.sparkContext
    )
    val eventKuduDS = SparkUtil.rdd2EventDS(
      kuduContext
        .kuduRDD(spark.sparkContext, TableName.EVENT_WOS),
      spark
    )

    val outDS = extract(eventParquetDS.union(eventKuduDS))
    // 写入parquet
    outDS.write.parquet(
      DateUtil.formatDateString(FilePath.USER_TAG_1_ROS_PARQUET, date)
    )

    spark.close()
  }
}
