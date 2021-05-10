package com.beercafeguy.spark.sql

import com.beercafeguy.spark.commons.BeerSessionBuilder

object PivotDemoApp {
  def main(args: Array[String]): Unit = {

    val spark = BeerSessionBuilder.getSession()
    val sourceDF = spark.read.option("header", "true")
      .option("inferSchema", "true")
      .csv("data/mpg.csv")
    //sourceDF.show()

    //sourceDF.withColumnRenamed("manufacturer", "manuf").show(5)
    //sourceDF.groupBy("class", "year").avg("hwy").show(false)

    sourceDF.groupBy("class").pivot("year").avg("hwy").show()
  }
}
