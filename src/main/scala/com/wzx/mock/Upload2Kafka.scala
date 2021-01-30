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

  private def initProducer() ={
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

    new KafkaProducer[String, String](props)
  }

  def mock2kafka(
      producer: Producer[String, String],
      topicName: String,
      path: String,
      delay: Float
  ): Unit = {
    val source = Source.fromFile(path)
    for (line <- source.getLines()) {
      val data = new ProducerRecord[String, String](topicName, null, line)
      val res = producer.send(data).get()
      log.debug(s"send log: $data; res: $res")
      Thread.sleep((delay * 1000).toInt)
    }
  }

  def main(args: Array[String]): Unit = {
    val delay = args.headOption.map(_.toFloat).getOrElse(1f)
    val dataPath = args.lift(1).getOrElse("/home/wzx/data/weblogs")
    val topic = config.getString("wzx.topic.weblogs.name")
    log.info(s"delay= ${delay}s")
    log.info(s"data path = $dataPath")
    log.info(s"topic = $topic")

    val producer = initProducer()
    mock2kafka(producer, topic, dataPath, delay)
    producer.close()
  }

}
