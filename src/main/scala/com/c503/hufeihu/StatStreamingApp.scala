package com.c503.hufeihu

import com.c503.hufeihu.dao.{CategaryClickCountDAO, CategarySearchClickCountDAO}
import com.c503.hufeihu.domain.{CategaryClickCount, CategarySearchClickCount, ClickLog}
import com.c503.hufeihu.utils.{DateUtils, HBaseUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import scala.collection.mutable.ListBuffer

object StatStreamingApp {

  def main(args: Array[String]): Unit = {

    /**
     * Spark Streaming 配置
     */
    val conf: SparkConf = new SparkConf().setAppName("StudyTest").setMaster("local[*]")
    val streamingContext = new StreamingContext(conf, Seconds(2))

    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "139.9.133.159:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      //"auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics: Array[String] = Array("flumeTopic")

    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    val logs: DStream[String] =stream.map(_.value())

    //stream.map((record: ConsumerRecord[String, String]) => (record.key, record.value)).print()


    //清洗数据，过滤出无用数据
    var cleanLog: DStream[ClickLog] =logs.map((line: String) =>{
      println(line)
      var infos: Array[String] =line.split("\t")
      var url: String =infos(2).split(" ")(1)
      var categaryId=0
      //把爱奇艺的类目编号拿到了
      if (url.startsWith("www")){
        categaryId=url.split("/")(1).toInt

      }
      ClickLog(infos(0),DateUtils.parseToMinute(infos(1)),categaryId,infos(3),infos(4).toInt)
    }).filter((log: ClickLog) =>log.categaryId!=0)
    cleanLog.print()


    //每个类别的每天的点击量，保存收集数据到 HBase 里面

    cleanLog.map((log: ClickLog) =>{
      (log.time.substring(0,8)+"_"+log.categaryId,1)
    }).reduceByKey(_+_).foreachRDD((rdd: RDD[(String, Int)]) =>{
      rdd.foreachPartition((partitions: Iterator[(String, Int)]) =>{
        val list = new ListBuffer[CategaryClickCount]
        partitions.foreach((pair: (String, Int)) =>{
          list.append(CategaryClickCount(pair._1,pair._2))
          println(pair._1,pair._2)
        })
        CategaryClickCountDAO.save(list)
      })
    })

    //业务功能实现
    cleanLog.map((log: ClickLog) =>{
      val url: String = log.refer.replace("//","/")
      val splits: Array[String] =   url.split("/")
      var host =""
      if(splits.length > 2){
        host=splits(1)
      }
      (host,log.time,log.categaryId)
    }).filter((x: (String, String, Int)) =>x._1 != "").map((x: (String, String, Int)) =>{
      (x._2.substring(0,8)+"_"+x._1+"_"+x._3,1)
    }).reduceByKey((_: Int)+(_: Int)).foreachRDD((rdd: RDD[(String, Int)]) =>{
      rdd.foreachPartition((partions: Iterator[(String, Int)]) =>{
        val list = new ListBuffer[CategarySearchClickCount]
        partions.foreach((pairs: (String, Int)) =>{
          list.append(CategarySearchClickCount(pairs._1,pairs._2))
          println(pairs._1,pairs._2)
        })
        CategarySearchClickCountDAO.save(list)
      })
    })

    streamingContext.start()
    streamingContext.awaitTermination()
    }
}