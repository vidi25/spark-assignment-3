package edu.knoldus.configuration

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object GlobalObject {
  val sparkConf: SparkConf = new SparkConf().setAppName("spark-assignment-3").setMaster("local[*]")
  val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  val filePath: String = "src/main/resources/D1.csv"
}
