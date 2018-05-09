package com.beercafeguy.spark

import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka._


object KafkaDepartmentHitCounter {

  def main(args: Array[String]): Unit = {
    val master=args(0)
    val windowInterval=args(1).toInt
    val topicName=args(2);
    val hdfsLocation=args(3)
    val topicSet=Set(topicName)
    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> "kafka1.host.beercafeguy.com:9092,kafka2.host.beercafeguy.com:9092,kafka3.host.beercafeguy.com:9092"
    )

    val conf=new SparkConf().setAppName("KafkaDepartmentHitCounter").setMaster(master)
    val sc=new SparkContext(conf)
    val streamingContext=new StreamingContext(sc,Seconds(windowInterval))

    val logStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamingContext, kafkaParams, topicSet)
    val logs=logStream.map(log => log._2)
    val departments=logs.filter(message => {
      val m = message.split(" ")
      m(6).startsWith("/department/")
    }).map(dept => (dept.split(" ")(6).split("/")(2),1))

    val departmentsCount=departments.reduceByKey(_+_)
    departmentsCount.saveAsTextFiles(hdfsLocation)

    streamingContext.start()
    streamingContext.awaitTermination()


  }
}
