package com.wzx.extracting

import com.holdenkarau.spark.testing.DatasetSuiteBase
import com.wzx.entity.Profile
import com.wzx.util.DateUtil
import org.scalatest.{FunSuite, Matchers}

import scala.util.Random.shuffle

class NewRegisterExtractTest extends FunSuite with DatasetSuiteBase with Matchers {

  test("extract") {
    import sqlContext.implicits._

    val inputData = List(
      Profile(
        "113.140.11.123",
        "陕西省",
        "2016-11-10"
      ),
      Profile(
        "58.241.76.18",
        "江苏省",
        "2016-11-04"
      ),
      Profile(
        "199.30.25.88",
        "全球",
        "2016-11-03"
      )
    )
    val inputDS = sc
      .parallelize(shuffle(inputData))
      .toDS()

    val outputDS = NewRegisterExtract.extract(
      inputDS,
      DateUtil.parseDayFormat("2016-11-10")
    )

    val output = outputDS.collect()
    output should have size 2
    output should contain ("113.140.11.123")
    output should contain ("58.241.76.18")
  }

}
