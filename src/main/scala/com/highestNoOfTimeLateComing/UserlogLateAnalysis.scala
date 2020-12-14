package com.highestNoOfTimeLateComing

import org.apache.spark.sql.{DataFrame, SparkSession}

/***
  * Class performs Extract and Transformation to analyse userlog for late coming
  * @param sparkSession SparkSession
  */
class UserlogLateAnalysis(sparkSession: SparkSession) {

  /***
    * Reads data from hdfs to create DataFrame
    * @param hdfsFilePath String
    * @return DataFrame
    */
  def readFilesFromHDFS(hdfsFilePath: String): DataFrame = {
    val userlogsDF = sparkSession.read
      .option("inferSchema", value = true)
      .option("header", value = true)
      .csv(hdfsFilePath)
    userlogsDF
  }
}
