package com.beercafeguy.spark.ml.airbnb

import com.beercafeguy.spark.ml.commons.SessionBuilder
import org.apache.log4j.Logger
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.functions.{col, exp, log}

object LogScalePriceApp {

  @transient lazy val logger:Logger=Logger.getLogger(this.getClass.getSimpleName)
  def main(args: Array[String]): Unit = {

    val spark = SessionBuilder.getSession(this.getClass.getSimpleName)

    val filePath = "data/sf-airbnb/sf-airbnb-clean.parquet"
    val airbnbDF = spark.read.parquet(filePath)

    val Array(trainDF, testDF) = airbnbDF.randomSplit(Array(.8, .2), seed = 42)
    val logTrainDF = trainDF.withColumn("log_price", log(col("price")))
    val logTestDF = testDF.withColumn("log_price", log(col("price")))

    val rFormula = new RFormula()
      .setFormula("log_price ~ . - price")
      .setFeaturesCol("features")
      .setLabelCol("log_price")
      .setHandleInvalid("skip")

    val lr = new LinearRegression()
      .setLabelCol("log_price")
      .setPredictionCol("log_pred")

    val pipeline = new Pipeline().setStages(Array(rFormula, lr))
    val pipelineModel = pipeline.fit(logTrainDF)
    val predDF = pipelineModel.transform(logTestDF)

    val expDF = predDF.withColumn("prediction", exp(col("log_pred")))
    val regressionEvaluator = new RegressionEvaluator()
      .setPredictionCol("prediction")
      .setLabelCol("price")

    val rmse = regressionEvaluator.setMetricName("rmse").evaluate(expDF)
    val r2 = regressionEvaluator.setMetricName("r2").evaluate(expDF)
    println(s"RMSE is $rmse")
    println(s"R2 is $r2")
    println("*-"*80)
  }
}
