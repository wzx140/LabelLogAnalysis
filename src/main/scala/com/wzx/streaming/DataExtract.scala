package com.wzx.streaming

import com.typesafe.config.ConfigFactory
import com.wzx.common.{Constant, FilePath, TableName}
import com.wzx.entity.{Event, Profile}
import com.wzx.sink.KuduSink
import io.lemonlabs.uri.Url
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.slf4j.LoggerFactory
import java.util.Properties

object DataExtract {
  private val name = this.getClass.getName.stripSuffix("$")
  private val log = LoggerFactory.getLogger(name)
  private val config = ConfigFactory.load("application.conf")

  private def initProducer() = {
    val brokers = config.getString("wzx.topic.weblogs.brokers")
    val topic = config.getString("wzx.topic.weblogs.name")
    log.info(s"topic = $topic")
    val props = new Properties()
    props.setProperty("bootstrap.servers", brokers)
    props.setProperty("group.id", name)
    log.info(s"props = $props")

    new FlinkKafkaConsumer[String](
      topic,
      new SimpleStringSchema(),
      props
    )
  }

  def dataExtract(
      stream: DataStream[String]
  ): DataStream[(String, String, String, String)] = {
    stream
      .map { line =>
        val splits = line.split(" ")
        // 0  => 183.162.52.7
        // 1,2 => -
        // 3  => [2016-11-10
        // 4  => 00:01:02]
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
      .name("extract")
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

  def dataFormat(
      stream: DataStream[(String, String, String, String)]
  ): DataStream[Event] =
    stream
      .map { data =>
        val time = data._1
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
            case _: java.lang.NumberFormatException =>
              log.warn(s"can not recognize cms id: $url")
          }
        }

        Event(
          url,
          cmsType,
          cmsId,
          traffic,
          ip,
          time
        )
      }
      .name("format")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(
      new FsStateBackend(FilePath.STATE_BACKEND_PATH)
    )
    env.enableCheckpointing(5 * 60 * 1000)
    // get source
    val producer = initProducer()
    val stream = env.addSource(producer).name("kafka source")
    // extract and format stream
    val extractedDataStream = dataExtract(stream)
    val eventDataStream = dataFormat(extractedDataStream)
    val profileDataStream = eventDataStream
      .keyBy(_.ip)
      .flatMap(new ProfileMapper)
    // add sink
    val eventSink = new KuduSink[Event](
      config.getString("wzx.db.kudu.master_url"),
      TableName.EVENT_WOS,
      120
    )
    val profileSink = new KuduSink[Profile](
      config.getString("wzx.db.kudu.master_url"),
      TableName.PROFILE_WOS,
  120
    )
    eventDataStream.addSink(eventSink).name("event sink")
    profileDataStream.addSink(profileSink).name("profile sink")

    env.execute(name)
  }

}
