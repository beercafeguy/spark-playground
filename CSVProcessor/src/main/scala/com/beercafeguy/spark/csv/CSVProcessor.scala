package com.beercafeguy.spark.csv

import java.io.StringReader

import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.apache.spark.{SparkConf, SparkContext}

object CSVProcessor extends App {

  val conf=new SparkConf().setAppName("CSVProcessor").setMaster("local[*]")
  val sc=new SparkContext(conf)

  val flightDataRaw=sc.textFile(args(0))
  val header=flightDataRaw.first
  val flightData=flightDataRaw.filter(_!=header)
  val flightDataRDD=flightData.map(processDelimitedLine(_))

  println(flightDataRDD.count)

  //take the record and cover in required tuple
  def processDelimitedLine(record:String) ={
    val reader=new StringReader(record)
    val parser=new CSVParser(reader,CSVFormat.DEFAULT.
      withTrim().
      withDelimiter(','))
    val csvRecords = parser.getRecords
    val csvRecord=csvRecords.get(0)
    println(csvRecord)
    (csvRecord.get(0),csvRecord.get(1),csvRecord.get(2).toInt)
  }

}
