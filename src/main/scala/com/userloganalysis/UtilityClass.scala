package com.userloganalysis
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
      .master("local[*]")
      .getOrCreate()
    spark
  }
}
