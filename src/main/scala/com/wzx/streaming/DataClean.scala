package com.wzx.streaming

import com.ggstar.util.ip.IpHelper
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.slf4j.LoggerFactory
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import com.wzx.util.DateUtil

import java.util.Properties
import com.wzx.common.Constant
import com.wzx.entity.Event
import io.lemonlabs.uri.{Host, Url}

object DataClean {
  private val name = this.getClass.getName.stripSuffix("$")
  private val log = LoggerFactory.getLogger(name)
  private val config = ConfigFactory.load("application.conf")

  private def initProducer() = {
    val brokers = config.getString("wzx.topic.weblogs.brokers")
    val topic = config.getString("wzx.topic.weblogs.name")
    log.info(s"topic =$topic")
    val props = new Properties()
    props.setProperty("bootstrap.servers", brokers)
    props.setProperty("group.id", name)
    log.info(s"props: $props")

    new FlinkKafkaConsumer[String](
      "topic",
      new SimpleStringSchema(),
      props
    )
  }

  def dataExtract(stream: DataStream[String]) = {
    stream
      .map { line =>
        val splits = line.split(" ")
        // 0  => 183.162.52.7
        // 1,2 => -
        // 3  => [10/Nov/2016:00:01:02
        // 4  => +0800]
        // 5  => "POST
        // 6  => /api3/getadv
        // 7  => HTTP/1.1"
        // 8  => 200
        // 9  => 813
        // 10 => "www.imooc.com"
        // 11 => "-"
        // 12 => cid=0&timestamp=1478707261865&uid=2871142&marking=androidbanner&secrect=a6e8e14701ffe9f6063934780d9e2e6d&token=f51e97d1cb1a9caac669ea8acc162b96
        // 13 => "mukewang/5.0.0
        // 14 => (Android
        // 15 => 5.1.1;
        // 16 => Xiaomi
        // 17 => Redmi
        // 18 => 3
        // 19 => Build/LMY47V),Network
        // 20 => 2G/3G"
        // 22 => 10.100.134.244:80
        // 23 => 200
        // 24 => 0.027
        // 25 => 0.027
        val ip = splits(0)
        val time = (splits(3) + " " + splits(4))
          .dropRight(1)
          .drop(1)
        val traffic = splits(9)
        val url = splits(11).replaceAll("\"", "")

        (time, ip, url, traffic)
      }
      // filter url
      .filter { x =>
        var valid = false
        try {
          val host = Url.parse(x._3).toJavaURI.getHost
          valid = host == Constant.IMOOC_DOMAIN
        } catch {
          case e: Exception => log.warn(s"invalid url: ${x._3}", e)
        }

        valid
      }
      // filter intranet address
      .filter(_._2 != "10.100.0.1")
  }

  def dataFormat(stream: DataStream[(String, String, String, String)]) =
    stream.map { data =>
      val time = DateUtil.parseSlashFormat(data._1)
      val ip = data._2
      val url = data._3
      val traffic = data._4.toInt

      // parse cms type and id
      // http://www.imooc.com/code/547   ===>  code/547  547
      var cmsType = ""
      var cmsId = -1
      val paths = Url.parse(url).path.toRootless.parts.toArray
      if (paths.length < 2) {
        log.warn(s"can not recognize cms: $url")
      } else {
        cmsType = paths(0)
        if (
          cmsType != Constant.CODE_CMS
          && cmsType != Constant.ARTICLE_CMS
          && cmsType != Constant.LEARN_CMS
          && cmsType != Constant.VIDEO_CMS
        ) {
          log.warn(s"can not recognize cms type: $url")
        }

        try {
          cmsId = paths(1).toInt
        } catch {
          case e: java.lang.NumberFormatException =>
            log.warn(s"can not recognize cms id: $url")
        }
      }
      // parse ip
      val city = IpHelper.findRegionByIp(ip)
      val day = DateUtil.getDay(time)

      Event(url, cmsType, cmsId, traffic, ip, city, DateUtil.formatLine(time), day)
    }

  def profileExtract(): Unit = {}

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE)
    val producer = initProducer()
    val stream = env.addSource(producer)
    val extractedDataStream = dataExtract(stream)
    val formattedDataStream = dataFormat(extractedDataStream)

    env.execute(name)
  }

}
