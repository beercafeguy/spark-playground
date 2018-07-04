package com.beercafeguy.spark

import org.apache.spark.AccumulatorParam

class EvenAccumulator extends AccumulatorParam[Int]{
  override def addInPlace(r1: Int, r2: Int): Int = {
    if(r1%2 == 0 && r2%2 ==0 )
      r1+r2
    else
      0
  }

  override def zero(initialValue: Int): Int = {
    initialValue+1
  }
}
