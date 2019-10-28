package com.atguigu

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.{BufferedSource, Source}

object ProducerDataToKafka {

  def writeData2Kafka(topic: String): Unit = {

    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val kafkaProducer: KafkaProducer[String, String] = new KafkaProducer[String, String](properties)
    val source: BufferedSource = Source.fromFile("E:\\code\\idea\\User-Behavior-Analysis\\Hot-Items-Analysis\\src\\main\\resources\\UserBehavior.csv")
    for (line <- source.getLines()) {
      val record: ProducerRecord[String, String] = new ProducerRecord[String, String](topic, line)
      kafkaProducer.send(record)
    }
    kafkaProducer.close()
  }

  def main(args: Array[String]): Unit = {
    writeData2Kafka("hotitems")
  }
}
