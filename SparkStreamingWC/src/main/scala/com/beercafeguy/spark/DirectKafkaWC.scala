package com.beercafeguy.spark

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object DirectKafkaWC {

  def main(args:Array[String]):Unit={
    val sparkConf=new SparkConf().setMaster("local[2]").setAppName("StreamingWC")
    val ssc=new StreamingContext(sparkConf,Seconds(2))

    val topicSet=Set("azure_case_data","azure_case_data_1")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "kafka1:9092,kafka2:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "fin_group_azure",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val stream=KafkaUtils.createDirectStream[String,String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topicSet, kafkaParams)
    )

    val message=stream.map(record => (record.key, record.value))
    val words=message.map(x => x._2).flatMap(_.split(" "))
    val wordCounts=words.map(word => (word,1)).reduceByKey((x,y) => x+y)
    wordCounts.print

    ssc.start()
    ssc.awaitTermination()
  }
}
