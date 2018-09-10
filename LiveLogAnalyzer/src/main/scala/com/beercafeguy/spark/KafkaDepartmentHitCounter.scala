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
    val bootstrapServers=args(4)
    val topicSet=Set(topicName)
    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> bootstrapServers,
      "zookeeper.connect" -> "or1hdp600.corp.adobe.com:2181,or1hdp601.corp.adobe.com:2181,or1hdp609.corp.adobe.com:2181",
      "group.id" -> "spark-streaming-test",
      "zookeeper.connection.timeout.ms" -> "1000"
    )

    val conf=new SparkConf().setAppName("KafkaDepartmentHitCounter").setMaster(master)
    val sc=new SparkContext(conf)
    val streamingContext=new StreamingContext(sc,Seconds(windowInterval))

    val logStream = KafkaUtils.
      createDirectStream[String, String, StringDecoder, StringDecoder](streamingContext, kafkaParams, topicSet)
    val logs=logStream.map(log => log._2)
    logs.saveAsTextFiles(hdfsLocation)
    /*val departments=logs.filter(message => {
      val m = message.split(" ")
      m(6).startsWith("/department/")
    }).map(dept => (dept.split(" ")(6).split("/")(2),1))

    val departmentsCount=departments.reduceByKey(_+_)
    departmentsCount.saveAsTextFiles(hdfsLocation)*/

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
