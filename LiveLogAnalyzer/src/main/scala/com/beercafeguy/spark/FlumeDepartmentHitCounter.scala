package com.beercafeguy.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FlumeDepartmentHitCounter {
  def main(args: Array[String]): Unit = {
    val master=args(0)
    val windowInterval=args(1).toInt
    val hostname=args(2)
    val portNumber=args(3).toInt
    val hdfsLocation=args(4)
    val conf=new SparkConf().setAppName("FlumeDepartmentHitCounter").setMaster(master)
    val streamingContext=new StreamingContext(conf,Seconds(windowInterval))

    val logs = FlumeUtils.createPollingStream(streamingContext,hostname, portNumber)
    val departments=logs.filter( message => {
      val m=new String(message.event.getBody.array()).split(" ")
      m(6).startsWith("/department/")
    }).map(dept => (new String(dept.event.getBody.array()).split(" ")(6).split("/")(2),1))

    val departmentsCount=departments.reduceByKey(_+_)
    departmentsCount.saveAsTextFiles(hdfsLocation)

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
