package com.beercafeguy.spark

import org.apache.spark.Partitioner

class MyPartitioner (override val numPartitions:Int)extends Partitioner{

  override def getPartition(key: Any): Int = {
    val k=key.asInstanceOf[Int]
    k%numPartitions
  }

  override def equals(other:scala.Any): Boolean = {
    other match {
      case obj: MyPartitioner => obj.numPartitions == numPartitions
      case _ =>  false
    }
  }
}
