package com.beercafeguy.spark.sql

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

object UserApp {
  def main(args: Array[String]): Unit = {

    val inputPath="data/test_user.csv"
    val outputPath="data_op/user/"
    val spark=SparkSession.builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    import spark.implicits._

    val sourceDF=spark.read
      .option("header","true")
      .option("delimiter","|")
      .option("inferSchema","true")
      .csv(inputPath)

    val schema=sourceDF.schema
    val intCols=getIntCols(schema)

    /*val windowSpec=Window.partitionBy("id")
      .orderBy($"age".asc)
    val filteredDF=sourceDF
      .withColumn("row_num",row_number() over(windowSpec))
      .filter($"row_num"===1)
      .drop("row_num")

    //filteredDF.show(false)

      val processedDF=filteredDF.withColumn("status",
      when($"age" < 30,"YOUNG").otherwise("OLD"))

    processedDF.printSchema()
    processedDF.coalesce(1)
      .write
      .option("header","true")
      .option("delimiter","|")
      .mode(SaveMode.Overwrite)
      .csv(outputPath)*/
  }

  def getIntCols(schema:StructType):Array[String]={
    val cols=schema
      .fields
      .filter(field => field.dataType.typeName.equalsIgnoreCase("integer"))
      .map(field => field.name)
      .toArray
    cols
  }
}
