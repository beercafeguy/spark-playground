package com.beercafeguy.spark.commons

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object BeerSessionBuilder {

  def getSession(appName:String): SparkSession = {
    SparkSession.builder
      .master("local[*]")
      .config(getConf())
      .appName(appName)
      .getOrCreate()

  }
  def getSession(): SparkSession = {
    getSession("Simple Spark App")
  }

  private def getConf(): SparkConf = {
    val conf: SparkConf = new SparkConf()
    conf.set("spark.sql.shuffle.partitions", "4")
    conf
  }


}
