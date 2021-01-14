package com.wzx.mock

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}
import org.slf4j.LoggerFactory

import scala.io.Source
import java.util.Properties

object Upload2Kafka {
  private val name = this.getClass.getName.stripSuffix("$")
  private val log = LoggerFactory.getLogger(name)
  private val config = ConfigFactory.load("application.conf")

  def main(args: Array[String]): Unit = {
    val delay = args.headOption.map(_.toInt).getOrElse(2)
    val dataPath = args.lift(1).getOrElse("~/data/weblogs")
    log.info(s"delay=$delay s")
    log.info(s"data path =$dataPath")
    // init producer
    val topic = config.getString("wzx.topic.weblogs.name")
    val brokers = config.getString("wzx.topic.weblogs.brokers")
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("client.id", name)
    props.put(
      "key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer"
    )
    props.put(
      "value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer"
    )
    log.info(s"props: $props")
    val producer = new KafkaProducer[String, String](props)
    mock2kafka(producer, topic, dataPath, delay)
    producer.close()
  }

  def mock2kafka(
      producer: Producer[String, String],
      topicName: String,
      path: String,
      delay: Int
  ): Unit = {
    val source = Source.fromFile(path)
    for (line <- source.getLines()) {
      val data = new ProducerRecord[String, String](topicName, null, line)
      producer.send(data)
      log.debug(s"send log: $data")
      Thread.sleep(delay * 1000)
    }
  }

}
