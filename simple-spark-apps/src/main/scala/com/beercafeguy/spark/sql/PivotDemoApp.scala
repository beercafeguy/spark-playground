package com.beercafeguy.spark.sql

import com.beercafeguy.spark.commons.BeerSessionBuilder

object PivotDemoApp {
  def main(args: Array[String]): Unit = {

    val spark = BeerSessionBuilder.getSession()
    val sourceDF = spark.read.option("header", "true")
      .option("inferSchema", "true")
      .csv("data/mpg.csv")
    sourceDF.show()
  }
}
