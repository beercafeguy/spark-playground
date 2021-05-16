package com.beercafeguy.spark.ml.airbnb

import com.beercafeguy.spark.ml.commons.SessionBuilder
import org.apache.log4j.Logger
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression

object LinearRegressionApp {

  @transient lazy val logger:Logger=Logger.getLogger(this.getClass.getSimpleName)
  def main(args: Array[String]): Unit = {

    val spark=SessionBuilder.getSession(this.getClass.getSimpleName)
    import spark.implicits._

    val filePath = "data/sf-airbnb/sf-airbnb-clean.parquet"
    val airbnbDF = spark.read.parquet(filePath)
    //println("count in source:"+airbnbDF.count)

    // Split data in training and test data
    val Array(trainDF, testDF) = airbnbDF.randomSplit(Array(.8, .2), seed=42)
    println(f"There are ${trainDF.cache().count()} rows in the training set, and ${testDF.cache().count()} in the test set")

    //trainDF.select("price", "bedrooms").summary().show(false)

    // Transformation -> 1
    val vecAssembler = new VectorAssembler()
      .setInputCols(Array("bedrooms"))
      .setOutputCol("features")

    val vecTrainDF = vecAssembler.transform(trainDF)
    //vecTrainDF.select("bedrooms", "features", "price").show(10)

    // Estimator -> 1
    val lr = new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol("price")

    val lrModel=lr.fit(vecTrainDF)

    val m=lrModel.coefficients(0)
    val b=lrModel.intercept
    println(f"The formula for the linear regression line is price = $m%1.2f*bedrooms + $b%1.2f")
    println("*-"*80)

    // Building pipeline

    val pipeline = new Pipeline().setStages(Array(vecAssembler, lr))
    val pipelineModel = pipeline.fit(trainDF)

    val predDF=pipelineModel.transform(testDF)
    predDF.select("bedrooms", "features", "price", "prediction").show(10)

    pipelineModel.write.save("model/single_feature_lr")
  }
}
