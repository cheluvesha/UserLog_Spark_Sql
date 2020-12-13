package com.Utility

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/***
  * Utility class which creates Utility functions
  */
object UtilityClass {

  /***
    * Creates SparkSession Object
    * @param appName String
    * @return SparkSession
    */
  def createSparkSessionObj(appName: String): SparkSession = {

    val spark = SparkSession
      .builder()
      .appName(appName)
      .config("spark.driver.host", "localhost")
      .master("local[*]")
      .getOrCreate()
    spark
  }
}
