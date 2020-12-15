package com.highestNoOfTimeLateComing

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/***
  * Class performs Extract and Transformation to analyse userlog for late coming
  * @param sparkSession SparkSession
  */
class UserlogLateAnalysis(sparkSession: SparkSession) {

  sparkSession.sparkContext.setLogLevel("OFF")

  /** *
    * Reads data from hdfs to create DataFrame
    *
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

  /** *
    * Selects the required rows from the DataFrame
    *
    * @param userlogsDF DataFrame
    * @return DataFrame
    */
  def selectRequiredColumn(userlogsDF: DataFrame): DataFrame = {
    try {
      val selectedColDF = userlogsDF.select(
        col("Datetime"),
        to_date(col("Datetime"), "yyyy-MM-dd") as "dates",
        col("user_name")
      )
      selectedColDF
    } catch {
      case sqlException: org.apache.spark.sql.AnalysisException =>
        throw new Exception("SQL Syntax Error Please Check The Syntax")
      case ex: Exception =>
        throw new Exception("Unable to split and create userlog dataframe")
    }
  }

  /** *
    * finds minimum time i.e Login time through Group by and creates loginTimeDF as DataFrame
    *
    * @param selectedColDF DataFrame
    * @return DataFrame
    */
  def findLoginTimeForUsers(selectedColDF: DataFrame): DataFrame = {
    try {
      val loginTimeDF = selectedColDF
        .groupBy("user_name", "dates")
        .agg(min("Datetime") as "users_login_time")
      loginTimeDF
    } catch {
      case sqlException: org.apache.spark.sql.AnalysisException =>
        throw new Exception("SQL Syntax Error Please Check The Syntax")
      case ex: Exception =>
        throw new Exception("Unable to find login time to create DataFrame")
    }
  }

  /**
    * Appends actual login time to Date column
    *
    * @param loginTimeDF     DataFrame
    * @param actualLoginTime String
    * @return DataFrame
    */
  def appendActualLoginTimeToDate(
      loginTimeDF: DataFrame,
      actualLoginTime: String
  ): DataFrame = {
    try {
      val actualTimeDF =
        loginTimeDF.withColumn("actual_login_time", lit(" " + actualLoginTime))
      val appendTimeDF = actualTimeDF.select(
        col("user_name"),
        (concat(col("dates"), col("actual_login_time")) as "entry_time")
          .cast("timestamp"),
        col("users_login_time")
      )
      appendTimeDF
    } catch {
      case sqlException: org.apache.spark.sql.AnalysisException =>
        throw new Exception("SQL Syntax Error Please Check The Syntax")
      case ex: Exception =>
        throw new Exception("Unable to find login time to create DataFrame")
    }
  }

  /** *
    * Finds late coming by comparing user login time with actual login time
    *
    * @param appendTimeDF DataFrame
    * @return DataFrame
    */
  def findLateComing(appendTimeDF: DataFrame): DataFrame = {
    try {
      val lateComingDF = appendTimeDF.select(
        col("*"),
        expr(
          "case when users_login_time > entry_time then 1 else 0 end"
        ) as "count_of_late"
      )
      val countLateComingDF = lateComingDF
        .groupBy("user_name")
        .agg(sum("count_of_late") as "NoOfTimeLateComing")
      countLateComingDF
    } catch {
      case sqlException: org.apache.spark.sql.AnalysisException =>
        throw new Exception("SQL Syntax Error Please Check The Syntax")
      case ex: Exception =>
        throw new Exception("Unable to find login time to create DataFrame")
    }
  }

  /** *
    * Finds highest number of times late comers by sorting with Descending
    *
    * @param noOfTimesLateComingDF DataFrame
    * @return DataFrame
    */
  def findHighestNoOfTimeLateComers(
      noOfTimesLateComingDF: DataFrame
  ): DataFrame = {
    try {
      val highestNoOfTimeDF =
        noOfTimesLateComingDF.sort(desc("NoOfTimeLateComing"))
      highestNoOfTimeDF
    } catch {
      case sqlException: org.apache.spark.sql.AnalysisException =>
        throw new Exception("SQL Syntax Error Please Check The Syntax")
      case ex: Exception =>
        throw new Exception("Unable to find login time to create DataFrame")
    }
  }

  def findUsersLateHours(appendLoginTimeDF: DataFrame): DataFrame = {
    try {
      appendLoginTimeDF.createTempView("user_login_table")
      val usersLateHourDF = sparkSession.sql(
        """select user_name,case when users_login_time > entry_time then (unix_timestamp(users_login_time)-unix_timestamp(entry_time))/(3600)) else 0.0 end as late_hours from user_login_table"""
      )
      usersLateHourDF
    } catch {
      case sqlException: org.apache.spark.sql.AnalysisException =>
        throw new Exception("SQL Syntax Error Please Check The Syntax")
      case ex: Exception =>
        throw new Exception("Unable to calculate daily hour")
    }
  }

}
