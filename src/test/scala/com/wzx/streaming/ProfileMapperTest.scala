package com.wzx.streaming

import com.wzx.entity.{Event, Profile}
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.operators.StreamFlatMap
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.{
  KeyedOneInputStreamOperatorTestHarness,
  OneInputStreamOperatorTestHarness
}
import org.scalactic.source.TypeInfo
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import collection.JavaConverters._

class ProfileMapperTest extends AnyFunSuite with BeforeAndAfter with Matchers {
  private var testHarness
      : KeyedOneInputStreamOperatorTestHarness[String, Event, Profile] = _
  private var statefulFlatMap: ProfileMapper = _

  before {
    //instantiate user-defined function
    statefulFlatMap = new ProfileMapper

    // wrap user defined function into a the corresponding operator
    testHarness = new KeyedOneInputStreamOperatorTestHarness(
      new StreamFlatMap(statefulFlatMap),
      new KeySelector[Event, String] {
        override def getKey(value: Event): String = {
          value.ip
        }
      },
      TypeInformation.of(classOf[String])
    )

    // open the test harness (will also call open() on RichFunctions)
    testHarness.open()
  }

  test("ProfileMapper") {

    //push (timestamped) elements into the operator (and hence user defined function)
    testHarness.processElement(
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
      ),
      100
    )
    testHarness.processElement(
      Event(
        "http://www.imooc.com/code/75",
        "code",
        75,
        2152,
        "113.140.11.123",
        "2016-11-10 00:01:02",
        2016,
        11,
        10
      ),
      101
    )
    testHarness.processElement(
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
      ),
      102
    )
    testHarness.processElement(
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
      ),
      103
    )

    val array = testHarness.getOutput.asScala
      .map(x => x.asInstanceOf[StreamRecord[Profile]].getValue)
      .toArray
    array should have size 3
    array should contain(
      Profile(
        "113.140.11.123",
        "陕西省",
        "2016-11-10"
      )
    )
    array should contain(
      Profile(
        "58.241.76.18",
        "江苏省",
        "2016-11-10"
      )
    )
    array should contain(
      Profile(
        "199.30.25.88",
        "全球",
        "2016-11-10"
      )
    )
  }

}
