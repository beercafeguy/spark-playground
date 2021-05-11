package com.beercafeguy.spark.commons

import org.apache.spark.sql.{DataFrame, SparkSession}

object CommonRecipes {

  def getCsv(sparkSession: SparkSession,loc:String):DataFrame={
    sparkSession.read.option("header","true").option("inferSchema","true").csv(loc)
  }
}
