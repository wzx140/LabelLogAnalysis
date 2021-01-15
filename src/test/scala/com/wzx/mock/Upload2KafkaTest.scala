package com.wzx.mock

import org.scalatest.funsuite.AnyFunSuite
import collection.JavaConverters._
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.matchers.should.Matchers
import scala.collection.mutable.ArrayBuffer

class Upload2KafkaTest extends AnyFunSuite with Matchers {

  test("mock2kafka") {
    val recoursePath = this.getClass.getResource("/mock_data").getPath
    val mockProducer = new MockProducer[String, String](
      true,
      new StringSerializer,
      new StringSerializer
    )
    Upload2Kafka.mock2kafka(mockProducer, "test", recoursePath, 1)

    mockProducer
      .history()
      .asScala
      .map(x => (x.topic(), x.key(), x.value())) should equal(
      ArrayBuffer(
        ("test", null, "11aa22bb"),
        ("test", null, "22bb33cc"),
        ("test", null, "33dd44dd")
      )
    )
  }
}
