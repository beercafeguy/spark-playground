package com.beercafeguy.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SocketTextStreamWC {

  def main(args: Array[String]): Unit = {
    val executionMode=args(0)
    val hostAddress=args(1)
    val hostPort=args(2).toInt
    val windowInterval=args(3).toInt
    val conf=new SparkConf().setAppName("SocketTextStreamWC").setMaster(executionMode)
    val streamingContext=new StreamingContext(conf,Seconds(windowInterval))
    val lines=streamingContext.socketTextStream(hostAddress,hostPort)
    val words=lines.flatMap(_.split(" ")).map(word => (word,1))
    val wordCount=words.reduceByKey(_+_)
    wordCount.print()

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
