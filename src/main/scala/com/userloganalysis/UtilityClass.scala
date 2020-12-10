package com.userloganalysis
import org.apache.spark.sql.SparkSession

object UtilityClass {

  def createSparkSessionObj(appName: String): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()
    spark
  }
}
