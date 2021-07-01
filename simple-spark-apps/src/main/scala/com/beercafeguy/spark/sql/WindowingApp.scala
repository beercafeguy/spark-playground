package com.beercafeguy.spark.sql

import com.beercafeguy.spark.commons.BeerSessionBuilder
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object WindowingApp {

  def main(args: Array[String]): Unit = {
    val spark = BeerSessionBuilder.getSession()
    val storeData = spark.read.option("header", "true")
      .option("delimiter", "|")
      .csv("data/store_revenue.csv")

    import spark.implicits._
    //storeData.show(false)

    // get the change in revenue against last month
    val lagSpec = Window.partitionBy($"store_id").orderBy($"monthId")
    val laggedData = storeData.withColumn("last_revenue", lag($"revenue", 1) over (lagSpec))
      .filter($"last_revenue".isNotNull)
    laggedData.show(100)

    val leadedData=storeData.withColumn("next_revenue", lead($"revenue", 1) over (lagSpec))
      .filter($"next_revenue".isNotNull)
    leadedData.show(100)
  }
}


case class Salary(depName: String, empNo: Long, name: String,
                  salary: Long, hobby: Seq[String])