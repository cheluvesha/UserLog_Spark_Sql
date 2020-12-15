package com.highestNoOfTimeLateComing

import org.apache.spark.sql.functions.{col, min, to_date}
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
    try {
      val userlogsDF = sparkSession.read
        .option("inferSchema", value = true)
        .option("header", value = true)
        .csv(hdfsFilePath)
      userlogsDF
    } catch {
      case sqlAnalysisException: org.apache.spark.sql.AnalysisException =>
        throw new Exception("Please Check The Path Specified")
    }
  }

  /***
    * Selects the required rows from the DataFrame
    * @param userlogsDF DataFrame
    * @return DataFrame
    */
  def selectRequiredColumn(userlogsDF: DataFrame): DataFrame = {
    try {
      val selectedDF = userlogsDF.select(
        col("Datetime"),
        to_date(col("Datetime"), "yyyy-MM-dd") as "dates",
        col("user_name")
      )
      selectedDF
    } catch {
      case sqlException: org.apache.spark.sql.AnalysisException =>
        throw new Exception("SQL Syntax Error Please Check The Syntax")
      case ex: Exception =>
        throw new Exception("Unable to split and create userlog dataframe")
    }
  }

  /***
    * finds minimum time i.e Login time through Group by and creates loginTimeDF DataFrame
    * @param userLogsDataFrame DataFrame
    * @return DataFrame
    */
  def findLoginTimeForUsers(userLogsDataFrame: DataFrame): DataFrame = {
    try {
      val loginTimeDF = userLogsDataFrame
        .groupBy("user_name", "dates")
        .agg(min("Datetime") as "login_time")
      loginTimeDF
    } catch {
      case sqlException: org.apache.spark.sql.AnalysisException =>
        throw new Exception("SQL Syntax Error Please Check The Syntax")
      case ex: Exception =>
        throw new Exception("Unable to find login time to create DataFrame")
    }
  }

}
