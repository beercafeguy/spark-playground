package com.beercafeguy.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming._

object DepartmentHitCounter {

  def main(args:Array[String])={
    val master=args(0)
    val windowInterval=args(1).toInt
    val hostname=args(2)
    val portNumber=args(3).toInt
    val hdfsLocation=args(4)
    val conf=new SparkConf().setAppName("DepartmentHitCounter").setMaster(master)
    val streamingContext=new StreamingContext(conf,Seconds(windowInterval))

    val messages=streamingContext.socketTextStream(hostname,portNumber)
    //filter will extrect department messages
    val departments=messages.filter( message => {
      val m=message.split(" ")
      m(6).startsWith("/department/")
    }).map(dept => (dept.split(" ")(6).split("/")(2),1))

    val departmentsCount=departments.reduceByKey(_+_)
    departmentsCount.saveAsTextFiles(hdfsLocation)


    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
