package com.wzx.streaming

import com.wzx.entity.Event
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import scala.collection.mutable.ArrayBuffer

class DataCleanTest extends FunSuite with BeforeAndAfter with Matchers {

  val flinkCluster = new MiniClusterWithClientResource(
    new MiniClusterResourceConfiguration.Builder()
      .setNumberSlotsPerTaskManager(1)
      .setNumberTaskManagers(1)
      .build
  )

  before {
    flinkCluster.before()
  }

  after {
    flinkCluster.after()
  }

  test("dataExtract") {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // configure your log4j.properties environment
    env.setParallelism(1)

    // values are collected in a static variable
    StringTupleSink.values.clear()

    val dataStream = env.fromElements(
      "183.162.52.7 - - [10/Nov/2016:00:01:02 +0800] \"POST /api3/getadv HTTP/1.1\" 200 813 \"www.imooc.com\" \"-\" cid=0&timestamp=1478707261865&uid=2871142&marking=androidbanner&secrect=a6e8e14701ffe9f6063934780d9e2e6d&token=f51e97d1cb1a9caac669ea8acc162b96 \"mukewang/5.0.0 (Android 5.1.1; Xiaomi Redmi 3 Build/LMY47V),Network 2G/3G\" \"-\" 10.100.134.244:80 200 0.027 0.027",
      "10.100.0.1 - - [10/Nov/2016:00:01:02 +0800] \"HEAD / HTTP/1.1\" 301 0 \"117.121.101.40\" \"-\" - \"curl/7.19.7 (x86_64-redhat-linux-gnu) libcurl/7.19.7 NSS/3.16.2.3 Basic ECC zlib/1.2.3 libidn/1.18 libssh2/1.4.2\" \"-\" - - - 0.000",
      "117.35.88.11 - - [10/Nov/2016:00:01:02 +0800] \"GET /article/ajaxcourserecommends?id=124 HTTP/1.1\" 200 2345 \"www.imooc.com\" \"http://www.imooc.com/code/1852\" - \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36\" \"-\" 10.100.136.65:80 200 0.616 0.616",
      "117.35.88.11 - - [10/Nov/2016:00:01:02 +0800] \"GET /article/ajaxcourserecommends?id=124 HTTP/1.1\" 200 2345 \"www.imooc.com\" xxxdf/d.f - \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36\" \"-\" 10.100.136.65:80 200 0.616 0.616"
    )
    val extractedDataStream = DataClean.dataExtract(dataStream)
    extractedDataStream.addSink(new StringTupleSink)
    env.execute()

    StringTupleSink.values should equal(
      ArrayBuffer(
        (
          "10/Nov/2016:00:01:02 +0800",
          "117.35.88.11",
          "http://www.imooc.com/code/1852",
          "2345"
        )
      )
    )
  }

  test("dataFormat") {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // configure your log4j.properties environment
    env.setParallelism(1)

    // values are collected in a static variable
    EventSink.values.clear()

    val dataStream = env.fromElements(
      "113.140.11.123 - - [10/Nov/2016:00:01:02 +0800] \"POST /course/ajaxmediauser/ HTTP/1.1\" 200 54 \"www.imooc.com\" \"http://www.imooc.com/video/5915/0\" mid=5915&time=60.01200000000006&learn_time=284.9 \"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.79 Safari/537.36 Edge/14.14393\" \"-\" 10.100.134.244:80 200 0.029 0.029",
      "125.119.9.35 - - [10/Nov/2016:00:01:02 +0800] \"GET /course/getcomment?mid=75&page=1&r=0.9849991562829477 HTTP/1.1\" 200 2152 \"www.imooc.com\" \"http://www.imooc.com/code/75\" - \"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 UBrowser/5.7.16400.12 Safari/537.36\" \"-\" 10.100.136.64:80 200 0.853 0.853",
      "58.241.76.18 - - [10/Nov/2016:00:01:02 +0800] \"GET /video/3235 HTTP/1.1\" 200 7228 \"www.imooc.com\" \"http://www.imooc.com/learn/175\" - \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36\" \"-\" 10.100.134.244:80 200 0.275 0.275",
      "199.30.25.88 - - [10/Nov/2016:00:01:03 +0800] \"GET /article/getuser?uid=2569618 HTTP/1.1\" 200 271 \"www.imooc.com\" \"http://www.imooc.com/article/11325\" - \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534+ (KHTML, like Gecko) BingPreview/1.0b\" \"-\" 10.100.136.64:80 200 0.076 0.076"
    )
    DataClean
      .dataFormat(DataClean.dataExtract(dataStream))
      .addSink(new EventSink)
    env.execute()

    val list = EventSink.values
    list should have size 4
    list should contain(
      Event(
        "http://www.imooc.com/video/5915/0",
        "video",
        5915,
        54,
        "113.140.11.123",
        "2016-11-10 00:01:02",
        2016,
        11,
        10
      )
    )
    list should contain(
      Event(
        "http://www.imooc.com/code/75",
        "code",
        75,
        2152,
        "125.119.9.35",
        "2016-11-10 00:01:02",
        2016,
        11,
        10
      )
    )
    list should contain(
      Event(
        "http://www.imooc.com/learn/175",
        "learn",
        175,
        7228,
        "58.241.76.18",
        "2016-11-10 00:01:02",
        2016,
        11,
        10
      )
    )
    list should contain(
      Event(
        "http://www.imooc.com/article/11325",
        "article",
        11325,
        271,
        "199.30.25.88",
        "2016-11-10 00:01:03",
        2016,
        11,
        10
      )
    )
  }
}

class StringTupleSink extends SinkFunction[(String, String, String, String)] {

  override def invoke(
      value: (String, String, String, String),
      context: SinkFunction.Context[_]
  ): Unit = {
    StringTupleSink.values += value
  }
}

object StringTupleSink {
  // must be static
  val values: ArrayBuffer[(String, String, String, String)] = ArrayBuffer()
}

class EventSink extends SinkFunction[Event] {

  override def invoke(
      value: Event,
      context: SinkFunction.Context[_]
  ): Unit = {
    EventSink.values += value
  }
}

object EventSink {
  // must be static
  val values: ArrayBuffer[Event] = ArrayBuffer()
}
