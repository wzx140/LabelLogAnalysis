package com.wzx.extracting

import com.holdenkarau.spark.testing.DatasetSuiteBase
import com.wzx.entity.Event
import org.scalatest.FunSuite
import scala.util.Random.shuffle

class VideoVisitOver100Test extends FunSuite with DatasetSuiteBase {

  test("extract") {
    import sqlContext.implicits._

    val inputData = List.fill(100)(
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
    ) ++
      List.fill(100)(
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
      ) ++
      List.fill(99)(
        Event(
          "http://www.imooc.com/video/11325/0",
          "video",
          11325,
          271,
          "199.30.25.88",
          "2016-11-10 00:01:03",
          2016,
          11,
          10
        )
      )
    val inputDS = sc
      .parallelize(shuffle(inputData))
      .toDS()
    val outputDS = sc
      .parallelize(List("113.140.11.123"))
      .toDS()

    val resDS = VideoVisitOver100.extract(inputDS)
    assertDatasetEquals(outputDS, resDS)
  }
}
