package com.beercafeguy.spark.sql

import org.apache.spark.sql.functions._
import com.beercafeguy.spark.commons.BeerSessionBuilder
import org.apache.spark.sql.expressions.Window

object RunningTotal {

  def main(args: Array[String]): Unit = {

    val spark = BeerSessionBuilder.getSession()
    val storeData = spark.read.option("header", "true")
      .option("delimiter", "|")
      .csv("data/store_revenue.csv")

    import spark.implicits._

    storeData.withColumn("running_revenue",sum($"revenue") over ( Window.partitionBy($"monthId").orderBy($"store_id")))
      .show(false)

  }
}
