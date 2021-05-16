package com.beercafeguy.spark.sql

import com.beercafeguy.spark.commons.{BeerSessionBuilder, CommonRecipes}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.to_date
object WriteToDeltaApp {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  val deltaPath="/football/matches_delta"
  def main(args: Array[String]): Unit = {
    logger.info("Creating session")
    val spark=BeerSessionBuilder.getSession()
    logger.info("Session created")
    import spark.implicits._

    val matches=CommonRecipes.getCsv(spark,"data/matches.csv")
      .withColumn("dt",to_date($"date")).drop("date")

    matches.show(false)
    matches.write.format("delta").save(deltaPath)

    spark.read.format("delta").load(deltaPath)
      .createOrReplaceTempView("tmp_foot_delta")

    spark.sql("select count(*) from tmp_foot_delta").show()
  }
}
