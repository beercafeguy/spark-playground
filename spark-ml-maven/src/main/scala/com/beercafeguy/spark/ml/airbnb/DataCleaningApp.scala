package com.beercafeguy.spark.ml.airbnb

import com.beercafeguy.spark.ml.commons.SessionBuilder
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, translate, when}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.ml.feature.Imputer

object DataCleaningApp {

  @transient lazy val logger:Logger=Logger.getLogger(this.getClass.getSimpleName)

  def main(args: Array[String]): Unit = {
    val spark=SessionBuilder.getSession(this.getClass.getSimpleName)

    import spark.implicits._

    val filePath = "data/airbnb/sf-airbnb.csv"

    val rawDF = spark.read
      .option("header", "true")
      .option("multiLine", "true")
      .option("inferSchema", "true")
      .option("escape", "\"")
      .csv(filePath)
    //rawDF.show()

    val baseDF = rawDF.select(
      "host_is_superhost",
      "cancellation_policy",
      "instant_bookable",
      "host_total_listings_count",
      "neighbourhood_cleansed",
      "latitude",
      "longitude",
      "property_type",
      "room_type",
      "accommodates",
      "bathrooms",
      "bedrooms",
      "beds",
      "bed_type",
      "minimum_nights",
      "number_of_reviews",
      "review_scores_rating",
      "review_scores_accuracy",
      "review_scores_cleanliness",
      "review_scores_checkin",
      "review_scores_communication",
      "review_scores_location",
      "review_scores_value",
      "price")

    //logger.info("records in base DF: "+baseDF.cache().count)
    val fixedPriceDF = baseDF.withColumn("price", translate($"price", "$,", "").cast("double"))
    fixedPriceDF.cache()
    //fixedPriceDF.show()
    //fixedPriceDF.describe().show(false)
    //fixedPriceDF.summary().show(false)
    val noNullsDF = fixedPriceDF.na.drop(cols = Seq("host_is_superhost"))
    val integerColumns = for (x <- baseDF.schema.fields if (x.dataType == IntegerType)) yield x.name
    var doublesDF = noNullsDF
    for (c <- integerColumns)
      doublesDF = doublesDF.withColumn(c, col(c).cast("double"))

    val columns = integerColumns.mkString("\n - ")
    logger.info(s"Columns converted from Integer to Double:\n - $columns \n")
    logger.info("*-"*80)

    val imputeCols = Array(
      "bedrooms",
      "bathrooms",
      "beds",
      "review_scores_rating",
      "review_scores_accuracy",
      "review_scores_cleanliness",
      "review_scores_checkin",
      "review_scores_communication",
      "review_scores_location",
      "review_scores_value"
    )

    for (c <- imputeCols)
      doublesDF = doublesDF.withColumn(c + "_na", when(col(c).isNull, 1.0).otherwise(0.0))

    doublesDF.summary().show(false)
    val imputer = new Imputer()
      .setStrategy("median")
      .setInputCols(imputeCols)
      .setOutputCols(imputeCols)

    val imputedDF = imputer.fit(doublesDF).transform(doublesDF)
    val posPricesDF = imputedDF.filter($"price" > 0)
    val cleanDF = posPricesDF.filter($"minimum_nights" <= 365)
    val outputPath = "data/sf-airbnb/sf-airbnb-clean.parquet"

    cleanDF.write.mode("overwrite").parquet(outputPath)
  }
}
