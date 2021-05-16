package com.beercafeguy.spark.ml.airbnb

import com.beercafeguy.spark.ml.commons.SessionBuilder
import org.apache.log4j.Logger
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.sql.functions.desc

object DecisionTreesApp {

  @transient lazy val logger:Logger=Logger.getLogger(this.getClass.getSimpleName)
  def main(args: Array[String]): Unit = {

    val spark = SessionBuilder.getSession(this.getClass.getSimpleName)
    import spark.implicits._
    val filePath = "data/sf-airbnb/sf-airbnb-clean.parquet"
    val airbnbDF = spark.read.parquet(filePath)

    val Array(trainDF, testDF) = airbnbDF.randomSplit(Array(.8, .2), seed = 42)

    val categoricalCols = trainDF.dtypes.filter(_._2 == "StringType").map(_._1)
    val indexOutputCols = categoricalCols.map(_ + "Index")

    val stringIndexer = new StringIndexer()
      .setInputCols(categoricalCols)
      .setOutputCols(indexOutputCols)
      .setHandleInvalid("skip")

    val numericCols = trainDF.dtypes.filter{ case (field, dataType) =>
      dataType == "DoubleType" && field != "price"}.map(_._1)
    // Combine output of StringIndexer defined above and numeric columns
    val assemblerInputs = indexOutputCols ++ numericCols
    val vecAssembler = new VectorAssembler()
      .setInputCols(assemblerInputs)
      .setOutputCol("features")


    val dt = new DecisionTreeRegressor().setLabelCol("price")
    val stages = Array(stringIndexer, vecAssembler, dt)
    val pipeline = new Pipeline()
      .setStages(stages)
    dt.setMaxBins(40)
    val pipelineModel = pipeline.fit(trainDF)
    val dtModel = pipelineModel.stages.last.asInstanceOf[org.apache.spark.ml.regression.DecisionTreeRegressionModel]
    println(dtModel.toDebugString)

    //get Imp features
    val featureImp = vecAssembler.getInputCols.zip(dtModel.featureImportances.toArray)
    val columns = Array("feature", "Importance")
    val featureImpDF = spark.createDataFrame(featureImp).toDF(columns: _*)
   // featureImpDF.orderBy($"Importance".desc).show()

    val predDF = pipelineModel.transform(testDF)

    predDF.select("features", "price", "prediction").orderBy(desc("price")).show(false)
  }
}
