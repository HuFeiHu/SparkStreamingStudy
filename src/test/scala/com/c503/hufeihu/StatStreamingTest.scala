package com.c503.hufeihu

import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.JavaConversions._


object StatStreamingTest {
  def main(args: Array[String]): Unit = {

    /**
     *Spark Streaming 配置
     */
    val conf: SparkConf = new SparkConf().setAppName("StudyTest").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(2))

    /**
     * Kafka参数配置
     */
    val TOPIC = "flumeTopic"
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"139.9.133.159:9092")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.GROUP_ID_CONFIG,"something")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000")

    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(Collections.singletonList(TOPIC))
    while ( true) {
      val records: ConsumerRecords[String, String] = consumer.poll(100)

      for (record <- records) {
        println(record.value())
      }
    }
    consumer.close()
  }
}
