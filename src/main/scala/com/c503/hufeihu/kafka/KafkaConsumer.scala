package com.c503.hufeihu.kafka

import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}


object KafkaConsumer {
  def main(args: Array[String]): Unit = {
    val TOPIC = "flumeTopic"
    val props = new Properties()
    props.put("bootstrap.servers", "139.9.133.159:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "something")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")

    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(Collections.singletonList(TOPIC))
    while ( true) {
      val records: ConsumerRecords[String, String] = consumer.poll(100)
      import scala.collection.JavaConversions._
      for (record <- records) {
       println(record.value())
      }
    }
    consumer.close()
  }
}
