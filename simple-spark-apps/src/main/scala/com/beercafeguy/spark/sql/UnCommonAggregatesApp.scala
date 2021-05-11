package com.beercafeguy.spark.sql

import com.beercafeguy.spark.commons.{BeerSessionBuilder, CommonRecipes}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{sum, to_date}

object UnCommonAggregatesApp {
  def main(args: Array[String]): Unit = {

    val spark=BeerSessionBuilder.getSession()
    import spark.implicits._

    val footballData=Seq(("Liverpool","Salah",1),
      ("Liverpool","Mane",2),
      ("M City","Sterling",1),
      ("M City","Kevin",1),
      ("Arsenal","Auba",2),
      ("Westham","Fornals",1),
      ("Chelsea","Warner",2),
      ("Liverpool","Firmino",1))

    val footballDF:DataFrame=footballData.toDF("club","player","goals")
    //footballDF.show(false)

    //total number of goals scored by each club
    //footballDF.groupBy("club").agg(sum($"goals").as("total_goals"))
    //  .orderBy($"total_goals".desc).show(false)

    val matches=CommonRecipes.getCsv(spark,"data/matches.csv")
      .withColumn("dt",to_date($"date")).drop("date")
    matches.show()
    matches.cube($"dt",$"club").count.show(false)


  }
}
