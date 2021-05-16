package com.beercafeguy.spark.ml.airbnb

import com.beercafeguy.spark.ml.commons.SessionBuilder
import org.apache.log4j.Logger
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}

object RandomForestApp {

  @transient lazy val logger:Logger=Logger.getLogger(this.getClass.getSimpleName)
  def main(args: Array[String]): Unit = {

    val spark = SessionBuilder.getSession(this.getClass.getSimpleName)

    val filePath = "data/sf-airbnb/sf-airbnb-clean.parquet"
    val airbnbDF = spark.read.parquet(filePath)

    val Array(trainDF, testDF) = airbnbDF.randomSplit(Array(.8, .2), seed = 42)

    val categoricalCols = trainDF.dtypes.filter(_._2 == "StringType").map(_._1)
    val indexOutputCols = categoricalCols.map(_ + "Index")

    val stringIndexer = new StringIndexer()
      .setInputCols(categoricalCols)
      .setOutputCols(indexOutputCols)
      .setHandleInvalid("skip")

    val numericCols = trainDF.dtypes.filter{ case (field, dataType) => dataType == "DoubleType" && field != "price"}.map(_._1)
    val assemblerInputs = indexOutputCols ++ numericCols
    val vecAssembler = new VectorAssembler()
      .setInputCols(assemblerInputs)
      .setOutputCol("features")

    val rf = new RandomForestRegressor()
      .setLabelCol("price")
      .setMaxBins(40)
      .setSeed(42)

    val pipeline = new Pipeline()
      .setStages(Array(stringIndexer, vecAssembler, rf))

    val paramGrid = new ParamGridBuilder()
      .addGrid(rf.maxDepth, Array(2, 4, 6))
      .addGrid(rf.numTrees, Array(10, 100))
      .build()

    val evaluator = new RegressionEvaluator()
      .setLabelCol("price")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)
      .setSeed(42)

    val cvModel = cv.fit(trainDF)
    cvModel.getEstimatorParamMaps.zip(cvModel.avgMetrics)

    val newCv = new CrossValidator()
      .setEstimator(rf)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)
      .setParallelism(4)
      .setSeed(42)

    val mergedPipeline = new Pipeline()
      .setStages(Array(stringIndexer, vecAssembler, newCv))

    val pipelineModel = mergedPipeline.fit(trainDF)

    val predDF = pipelineModel.transform(testDF)

    val regressionEvaluator = new RegressionEvaluator()
      .setPredictionCol("prediction")
      .setLabelCol("price")
      .setMetricName("rmse")

    val rmse = regressionEvaluator.evaluate(predDF)
    val r2 = regressionEvaluator.setMetricName("r2").evaluate(predDF)
    println(s"RMSE is $rmse")
    println(s"R2 is $r2")
    println("*-"*80)
  }
}
