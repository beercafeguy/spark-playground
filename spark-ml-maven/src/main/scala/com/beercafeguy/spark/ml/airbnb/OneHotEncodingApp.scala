package com.beercafeguy.spark.ml.airbnb

import com.beercafeguy.spark.ml.commons.SessionBuilder
import org.apache.log4j.Logger
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{OneHotEncoder, RFormula, StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.functions.{avg, lit}

object OneHotEncodingApp {

  @transient lazy val logger:Logger=Logger.getLogger(this.getClass.getSimpleName)
  def main(args: Array[String]): Unit = {

    val spark=SessionBuilder.getSession(this.getClass.getSimpleName)
    import spark.implicits._

    val filePath = "data/sf-airbnb/sf-airbnb-clean.parquet"
    val airbnbDF = spark.read.parquet(filePath)

    val Array(trainDF, testDF) = airbnbDF.randomSplit(Array(.8, .2), seed=42)

    val categoricalCols = trainDF.dtypes.filter(_._2 == "StringType").map(_._1)
    val indexOutputCols = categoricalCols.map(_ + "Index")
    val oheOutputCols = categoricalCols.map(_ + "OHE")

    val stringIndexer = new StringIndexer()
      .setInputCols(categoricalCols)
      .setOutputCols(indexOutputCols)
      .setHandleInvalid("skip")

    val oheEncoder = new OneHotEncoder()
      .setInputCols(indexOutputCols)
      .setOutputCols(oheOutputCols)

    val numericCols = trainDF.dtypes.filter{ case (field, dataType) =>
      dataType == "DoubleType" && field != "price"}.map(_._1)
    val assemblerInputs = oheOutputCols ++ numericCols

    val vecAssembler = new VectorAssembler()
      .setInputCols(assemblerInputs)
      .setOutputCol("features")

    // whole code block used for feature engineering above can be replaced with rFormula
    // But it has its own downsides so not using it for now
    /*val rFormula = new RFormula()
      .setFormula("price ~ .")
      .setFeaturesCol("features")
      .setLabelCol("price")
      .setHandleInvalid("skip")
     */



    val lr = new LinearRegression()
      .setLabelCol("price")
      .setFeaturesCol("features")
    val stages = Array(stringIndexer, oheEncoder, vecAssembler,  lr)

    val pipeline = new Pipeline()
      .setStages(stages)


    val pipelineModel = pipeline.fit(trainDF)
    val predDF = pipelineModel.transform(testDF)

    predDF.select("features", "price", "prediction").show(5)
    // here you will find the prediction is somewhere near price.
    // will calculate accuracy in next section

    // RSME Evaluation
    val regressionEvaluator = new RegressionEvaluator()
      .setPredictionCol("prediction")
      .setLabelCol("price")
      .setMetricName("rmse")

    val rmse = regressionEvaluator.evaluate(predDF)
    println(f"RMSE is $rmse%1.2f")

    // to validate the RSME which we get, let try to create a baseline model and get its RSME
    //Here we will predict the value for each record as AVG of proce column
    val avgPrice:Double=predDF.agg(avg($"price")).collect()(0).get(0).toString.toDouble

    val baselineDF=predDF.withColumn("base_prediction",lit(avgPrice))
    val baselineRSME=new RegressionEvaluator()
      .setPredictionCol("base_prediction")
      .setLabelCol("price")
      .setMetricName("rmse")
      .evaluate(baselineDF)
    println(f"RMSE is $rmse%1.2f whereas BASELINE RSME is $baselineRSME%1.2f")


    // R Square evaluation
    val r2=regressionEvaluator.setMetricName("r2").evaluate(predDF)
    print(f"R2 is $r2")

    //Save your model
    val modelPath="model/ohe_lr"
    pipelineModel.write.overwrite().save(modelPath)

    // Read the model
    PipelineModel.load(modelPath)
  }
}
