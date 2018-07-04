package com.beercafeguy.spark

import org.apache.spark.Partitioner

class DomainPartitioner extends Partitioner{
  override def numPartitions: Int = 4

  override def getPartition(key: Any): Int = {
    val customerId=key.asInstanceOf[Double].toInt
    if(customerId==20134 || customerId == 23534)
      return 0
    else
      return new java.util.Random().nextInt(2)+1
  }
}
