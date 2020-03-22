package com.c503.hufeihu.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

import scala.io.Source


object KafkaProduce{
  def main(args: Array[String]): Unit = {
    val kafkaProp = new Properties()
    kafkaProp.put("bootstrap.servers", "139.9.133.159:9092")
    kafkaProp.put("acks", "1")
    kafkaProp.put("retries", "3")
    //kafkaProp.put("batch.size", 16384)//16k
    kafkaProp.put("key.serializer", classOf[StringSerializer].getName)
    kafkaProp.put("value.serializer", classOf[StringSerializer].getName)
    val producer = new KafkaProducer[String, String](kafkaProp)
    val lines = Source.fromFile("C:\\Users\\HuFeiHu\\Desktop\\kafka-data.txt").getLines()
    while (lines.hasNext) {
      val line = lines.next()
      val record = new ProducerRecord[String, String]("flumeTopic", line)
      producer.send(record, new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          if (metadata != null) {
            println("发送成功")
          }
          if (exception != null) {
            println("消息发送失败")
          }
        }
      })
    }
    producer.close()
  }
}

