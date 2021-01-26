package com.wzx.util

import com.wzx.entity.{Event, Profile}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.flink.types
import org.apache.flink.streaming.api.scala._

object TransformUtil {

  def rdd2EventDS(rdd: RDD[Row], spark: SparkSession): Dataset[Event] = {
    import spark.implicits._
    rdd
      .map {
        case Row(
              ip: String,
              time: String,
              url: String,
              cms_type: String,
              cms_id: Long,
              traffic: Long
            ) =>
          Event(url, cms_type, cms_id, traffic, ip, time)
      }
      .toDS()
  }

  def event2Row(stream: DataStream[Event]): DataStream[types.Row] = {
    stream.map { x =>
      val Event(url, cms_type, cms_id, traffic, ip, time) = x
      val row = new types.Row(6)
      row.setField(0, ip)
      row.setField(1, time)
      row.setField(2, url)
      row.setField(3, cms_type)
      row.setField(4, cms_id)
      row.setField(5, traffic)

      row
    }
  }

  def profile2Row(stream: DataStream[Profile]): DataStream[types.Row] = {
    stream.map { x =>
      val Profile(ip, city, register_day) = x
      val row = new types.Row(4)
      row.setField(0, ip)
      row.setField(1, city)
      row.setField(2, register_day)

      row
    }
  }

}
