package com.highestAverageHours

/***
  * Dependencies used Spark Core and Spark Sql Api
  */
import java.sql.SQLSyntaxErrorException
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/***
  * Class reads data from mysql and creates DataFrame
  * in order to perform analysis and computation on UserLogs Data
  */
class HighestAverageHour(sparkSession: SparkSession) {

  lazy val logger: Logger = Logger.getLogger(getClass.getName)

  /***
    * Reads Data From Mysql Database and Creates DataFrame
    * @param dbName String
    * @param tableName String
    * @param username String
    * @param password String
    * @param url String
    * @return DataFrame
    */
  def readDataFromMySqlForDataFrame(
      dbName: String,
      tableName: String,
      username: String,
      password: String,
      url: String
  ): DataFrame = {
    logger.info("Read operation started to create dataframe from mysql")
    try {
      val userlogReadDF = sparkSession.read
        .format("jdbc")
        .option("url", url + dbName)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("dbtable", tableName)
        .option("user", username)
        .option("password", password)
        .load()
      logger.info("Read operation ended")
      userlogReadDF
    } catch {
      case nullPointerException: NullPointerException =>
        logger.error(nullPointerException.printStackTrace())
        throw new Exception(
          "Parameters Are Null,Please Check The Passed Parameters"
        )
      case sqlSyntaxErrorException: SQLSyntaxErrorException =>
        logger.error(sqlSyntaxErrorException.printStackTrace())
        throw new Exception(
          "Fault Database Credentials, Please Provide Proper DB and Table configurations"
        )
      case sparkAnalysisException: org.apache.spark.sql.AnalysisException =>
        logger.error(sparkAnalysisException.printStackTrace())
        throw new Exception(
          "Please Provide Existing Column Name to Select Column From DataFrame"
        )
      case ex: Exception =>
        logger.error(ex.printStackTrace())
        throw new Exception("Unable To Read Data From Mysql Database")
    }
  }

  /***
    * selects required column from Dataframe
    * @param userlogsDF DataFrame
    * @return DataFrame
    */
  def selectRequiredColumn(userlogsDF: DataFrame): DataFrame = {
    logger.info("Executing selectRequiredColumn")
    try {
      val userlogSelectDF =
        userlogsDF.select("datetime", "username", "keyboard", "mouse")
      userlogSelectDF
    } catch {
      case sparkAnalysisException: org.apache.spark.sql.AnalysisException =>
        logger.error(sparkAnalysisException.printStackTrace())
        throw new Exception(
          "Please Provide Existing Column Name to Select Column From DataFrame"
        )
    }
  }

  /***
    * splits timestamp column to create Date column and selects required column to create Dataframe
    * @param userDataFrame DataFrame
    * @return DataFrame
    */
  def splitAndCreateUserlogDF(userDataFrame: DataFrame): DataFrame = {
    logger.info("started splitAndCreateUserlogDF")
    try {
      val splitDF = userDataFrame.select(
        col("datetime"),
        to_date(col("datetime"), "yyyy-MM-dd") as "dates",
        col("username")
      )
      logger.info("table is created")
      splitDF
    } catch {
      case sqlException: org.apache.spark.sql.AnalysisException =>
        logger.error(sqlException.printStackTrace())
        throw new Exception("SQL Syntax Error Please Check The Syntax")
      case ex: Exception =>
        logger.error(ex.printStackTrace())
        throw new Exception("Unable to split and create userlog dataframe")
    }
  }

  /***
    * finds minimum time i.e Login time and maximum time i.e Logout time through Group by and creates minAndMaxDF DataFrame
    * @param userLogsDataFrame DataFrame
    * @return DataFrame
    */
  def findMinAndMaxTimeForUsers(userLogsDataFrame: DataFrame): DataFrame = {
    logger.info("Entered into function to find min and max time")
    try {
      val minAndMaxDF = userLogsDataFrame
        .groupBy("username", "dates")
        .agg(min("datetime") as "login", max("datetime") as "logout")
      minAndMaxDF
    } catch {
      case sqlException: org.apache.spark.sql.AnalysisException =>
        logger.error(sqlException.printStackTrace())
        throw new Exception("SQL Syntax Error Please Check The Syntax")
      case ex: Exception =>
        logger.error(ex.printStackTrace())
        throw new Exception("Unable to find min and max and create DataFrame")
    }
  }

  /***
    * calculates time duration between login and logout for particular day
    * @param minTimeDF DataFrame
    * @return DataFrame
    */
  def calculateDailyHour(minTimeDF: DataFrame): DataFrame = {
    logger.info("Calculating Daily Hour")
    try {
      val dailyHourDF = minTimeDF.select(
        col("username"),
        col("dates"),
        (unix_timestamp(col("logout")) - unix_timestamp(
          col("login")
        )) / 3600 as "work_durations"
      )
      dailyHourDF
    } catch {
      case sqlException: org.apache.spark.sql.AnalysisException =>
        logger.error(sqlException.printStackTrace())
        throw new Exception("SQL Syntax Error Please Check The Syntax")
      case ex: Exception =>
        logger.error(ex.printStackTrace())
        throw new Exception("Unable to calculate daily hour")
    }
  }

  /***
    * with respect to each user, sums the time durations by performing group by on users
    * @param dailyHourDF DataFrame
    * @return DataFrame
    */
  def sumDailyHourWRTUser(dailyHourDF: DataFrame): DataFrame = {
    logger.info("sums the time durations by performing group by on users")
    try {
      val summedDF = dailyHourDF
        .groupBy("username")
        .agg(round(sum("work_durations"), 2) as "totalhours")
      summedDF
    } catch {
      case sqlException: org.apache.spark.sql.AnalysisException =>
        logger.error(sqlException.printStackTrace())
        throw new Exception("SQL Syntax Error Please Check The Syntax")
      case ex: Exception =>
        logger.error(ex.printStackTrace())
        throw new Exception("Unable to sum daily userlog time")
    }
  }

  /***
    * Creates Broadcast variables from idle hours DataFrame
    * @param idleHoursDF DataFrame
    * @return Broadcast[collection.Map[String, Double]]
    */
  def createIdleHoursBroadCast(
      idleHoursDF: DataFrame
  ): Broadcast[collection.Map[String, Double]] = {
    logger.info("Executing createIdleHoursBroadCast")
    val idleHoursMap =
      idleHoursDF.rdd
        .map(users => (users.getString(0), users.getDouble(1)))
        .collectAsMap()
    val broadcastIdleHour = sparkSession.sparkContext.broadcast(idleHoursMap)
    broadcastIdleHour
  }

  /***
    * Calculates working hours by differencing idle hours
    * @param overAllHoursDF DataFrame
    * @param idleHoursBV Broadcast[collection.Map[String, Double]]
    * @param days Int
    * @return DataFrame
    */
  def findHourByDifferencingIdleHours(
      overAllHoursDF: DataFrame,
      idleHoursBV: Broadcast[collection.Map[String, Double]],
      days: Int
  ): DataFrame = {
    logger.info("Executing findHourByDifferencingIdleHours")
    val spark = sparkSession
    import spark.sqlContext.implicits._
    val columns = Seq("username", "average_hours")
    val averageHourDF = overAllHoursDF
      .map(row => {
        val username = row.getString(0)
        val totalHours = row.getDouble(1)
        val idleHours = idleHoursBV.value.getOrElse(username, 0.0)
        val overAllHours = (totalHours - idleHours) / days
        val roundedHours = BigDecimal(overAllHours)
          .setScale(2, BigDecimal.RoundingMode.HALF_UP)
          .toDouble
        (username, roundedHours)
      })
      .toDF(columns: _*)
    averageHourDF
  }

  /***
    * Orders the Average hours from highest to lowest
    * @param overAllAverageHourDF DataFrame
    * @return DataFrame
    */
  def sortByHighestAverageHours(overAllAverageHourDF: DataFrame): DataFrame = {
    logger.info("Executing sortByHighestAverageHours")
    try {
      overAllAverageHourDF.createTempView("overall_average_hours")
      val highestAverageHourDF = sparkSession.sql(
        """SELECT username,average_hours FROM overall_average_hours ORDER BY average_hours DESC"""
      )
      highestAverageHourDF
    } catch {
      case sqlException: org.apache.spark.sql.AnalysisException =>
        logger.error(sqlException.printStackTrace())
        throw new Exception("SQL Syntax Error Please Check The Syntax")
    }
  }
}
