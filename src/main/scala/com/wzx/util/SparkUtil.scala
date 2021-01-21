package com.wzx.util

import com.wzx.entity.Event
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkUtil {

  def rdd2EventDS(rdd: RDD[Row], spark: SparkSession): Dataset[Event] = {
    import spark.implicits._
    rdd.map {
        case Row(
              ip: String,
              year: Int,
              month: Int,
              day: Int,
              time: String,
              url: String,
              cms_type: String,
              cms_id: Long,
              traffic: Long
            ) =>
          Event(url, cms_type, cms_id, traffic, ip, time, year, month, day)
      }.toDS()
  }


}
