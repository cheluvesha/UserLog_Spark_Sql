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
        (concat(
          col("dates"),
          col("actual_login_time")
        ) as "entry_time")
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

  /***
    * finds late hours with respect to each users
    * @param appendLoginTimeDF DataFrame
    * @return DataFrame
    */
  def findUsersLateHours(appendLoginTimeDF: DataFrame): DataFrame = {
    try {
      appendLoginTimeDF.createTempView("user_login_table")
      val usersLateHourDF = sparkSession.sql(
        """select user_name,case when users_login_time > entry_time then (unix_timestamp(users_login_time)-unix_timestamp(entry_time))/(3600) else 0 end as late_hours from user_login_table"""
      )
      usersLateHourDF
    } catch {
      case sqlException: org.apache.spark.sql.AnalysisException =>
        throw new Exception("SQL Syntax Error Please Check The Syntax")
      case ex: Exception =>
        throw new Exception("Unable to calculate daily hour")
    }
  }

  /***
    * Sums the each day late hours by group by users
    * @param usersLateDF DataFrame
    * @return DataFrame
    */
  def sumLateHoursForEachUsers(usersLateDF: DataFrame): DataFrame = {
    try {
      val totalLateHoursDF = usersLateDF
        .groupBy(col("user_name"))
        .agg(sum("late_hours") as "total_late_hours")
      totalLateHoursDF
    } catch {
      case sqlException: org.apache.spark.sql.AnalysisException =>
        throw new Exception("SQL Syntax Error Please Check The Syntax")
    }
  }

  /***
    * Creates Broadcast variables for number of late coming data
    * @param noOfTimesLateDF DataFrame
    * @return Broadcast[collection.Map[String, Long]]
    */
  def broadcastNoOfTimesLateComing(
      noOfTimesLateDF: DataFrame
  ): Broadcast[collection.Map[String, Long]] = {
    try {
      val noOfLateMap =
        noOfTimesLateDF.rdd
          .map(users => (users.getString(0), users.getLong(1)))
          .collectAsMap()
      val broadcastNoOfLate = sparkSession.sparkContext.broadcast(noOfLateMap)
      broadcastNoOfLate
    } catch {
      case sqlAnalysisException: org.apache.spark.sql.AnalysisException =>
        throw new Exception("Unable to create broadcast variables")
    }
  }

  /***
    * Finds the average late hour
    * @param usersTotalLateHourDF DataFrame
    * @param broadcastNoOfTimesLate Broadcast[collection.Map[String, Long]]
    * @return DataFrame
    */
  def findAverageLateHours(
      usersTotalLateHourDF: DataFrame,
      broadcastNoOfTimesLate: Broadcast[collection.Map[String, Long]]
  ): DataFrame = {
    try {
      val spark = sparkSession
      import spark.sqlContext.implicits._
      val columns = Seq("username", "average_late_hours")
      val averageLateHourDF = usersTotalLateHourDF
        .map(row => {
          val username = row.getString(0)
          val lateHours = row.getDouble(1)
          val noOfLate = broadcastNoOfTimesLate.value.getOrElse(username, 0L)
          val overAllHours = lateHours / noOfLate
          val roundedAvgHours = BigDecimal(overAllHours)
            .setScale(2, BigDecimal.RoundingMode.HALF_UP)
            .toDouble
          (username, roundedAvgHours)
        })
        .toDF(columns: _*)
      averageLateHourDF
    } catch {
      case sqlAnalysisException: org.apache.spark.sql.AnalysisException =>
        throw new Exception("Unable to find average late hours")
    }
  }

  /***
    * finds the total number of working days
    * @param selectedColUserlogDF DataFrame
    * @return Int
    */
  def findNoOfWorkingDays(selectedColUserlogDF: DataFrame): Int = {
    try {
      val noOfWorkingDays =
        selectedColUserlogDF.select(countDistinct("dates")).take(1)
      var days = 0L
      noOfWorkingDays.foreach(row => days = row.getLong(0))
      days.toInt
    } catch {
      case sqlAnalysisException: org.apache.spark.sql.AnalysisException =>
        throw new Exception("Unable to find number of working days")
    }
  }

  /***
    * Finds the Users total number of leaves
    * @param noOfWorkingDays Int
    * @param userlogDF DataFrame
    * @return DataFrame
    */
  def findUsersTotalNumberOfLeaves(
      noOfWorkingDays: Int,
      userlogDF: DataFrame
  ): DataFrame = {
    try {
      userlogDF.createTempView("userlogs_leaves")
      val usersTotalNoOfAttendanceDF = sparkSession.sql(
        "select user_name, count(distinct(dates)) as attendance_count from userlogs_leaves group by user_name"
      )
      val addTotalNoWorkingDaysDF =
        usersTotalNoOfAttendanceDF.select(
          col("*"),
          lit(noOfWorkingDays) as "noOfWorkingDays"
        )
      val noOfLeavesDF = addTotalNoWorkingDaysDF.select(
        col("user_name"),
        (col("noOfWorkingDays") - col("attendance_count")) as "number_of_leaves"
      )
      noOfLeavesDF
    } catch {
      case sqlAnalysisException: org.apache.spark.sql.AnalysisException =>
        throw new Exception("Unable to find users total number of leaves")
    }
  }
}
