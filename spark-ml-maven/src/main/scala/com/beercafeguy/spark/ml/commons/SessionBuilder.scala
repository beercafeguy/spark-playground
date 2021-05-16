package com.beercafeguy.spark.ml.commons

import org.apache.spark.sql.SparkSession

object SessionBuilder {

  def getSession(appName:String):SparkSession={
    SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .config("spark.sql.shuffle.partitions",4)
      .getOrCreate()
  }

  def getSession(): SparkSession ={
    getSession("Simple Spark App")
  }
}
