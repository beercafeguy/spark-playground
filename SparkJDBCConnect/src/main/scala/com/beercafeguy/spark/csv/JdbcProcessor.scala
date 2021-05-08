package com.beercafeguy.spark.csv

import org.apache.spark.sql.{SaveMode, SparkSession}

object JdbcProcessor extends App {

  val spark = SparkSession.builder().appName("JDBC Spark App")
    .config("spark.sql.shuffle.partitions",4)
    .master("local[*]")
    .getOrCreate()

  val sourceData=spark.read
    .format("jdbc")
    .option("url","jdbc:mysql://localhost:33061/dbtest")
    .option("dbtable","dim_users")
    .option("driver","com.mysql.jdbc.Driver")
    .option("user","root")
    .option("password","password")
    .load()

  sourceData.write.format("jdbc")
    .option("url","jdbc:mysql://localhost:33061/dbtest")
    .option("dbtable","dim_users_parsed")
    .option("driver","com.mysql.jdbc.Driver")
    .option("user","root")
    .option("password","password")
    .mode(SaveMode.Overwrite)
    .save()
}
