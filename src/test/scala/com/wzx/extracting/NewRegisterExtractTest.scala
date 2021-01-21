package com.wzx.extracting

import com.holdenkarau.spark.testing.DatasetSuiteBase
import com.wzx.entity.Profile
import com.wzx.util.DateUtil
import org.scalatest.FunSuite
import scala.util.Random.shuffle

class NewRegisterExtractTest extends FunSuite with DatasetSuiteBase {

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
    val outputDS = sc
      .parallelize(List("113.140.11.123", "58.241.76.18"))
      .toDS()

    val resDS = NewRegisterExtract.extract(
      inputDS,
      DateUtil.parseDayFormat("2016-11-10")
    )

    assertDatasetEquals(resDS, outputDS)
  }

}
