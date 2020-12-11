package com.userloganalysis

/***
  * Dependencies used Spark Core and Spark Sql Api
  */
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{
  col,
  max,
  min,
  sum,
  to_date,
  unix_timestamp
}
import org.apache.spark.sql.{DataFrame, SparkSession}

/***
  * AverageLowestHour Class reads data from mysql and creates DataFrame
  * in order to perform analysis and computation on UserLogs Data
  */
class AverageLowestHour(sparkSession: SparkSession) {

  sparkSession.sparkContext.setLogLevel("OFF")
  lazy val logger: Logger = Logger.getLogger(getClass.getName)

  /***
    * Reads Data From Mysql Database and Creates DataFrame
    * @return DataFrame
    */
  def readDataFromMySqlForDataFrame(): DataFrame = {
    logger.info("Read operation started to create dataframe from mysql")
    try {
      val userlogReadDF = sparkSession.read
        .format("jdbc")
        .option("url", "jdbc:mysql://localhost:3306/fellowship")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("dbtable", "userlogs")
        .option("user", "user")
        .option("password", "user00")
        .load()
      logger.info("Read operation ended")
      val userlogDF =
        userlogReadDF.select("datetime", "username", "keyboard", "mouse")
      userlogDF
    } catch {
      case ex: Exception =>
        logger.error(ex.printStackTrace())
        throw new Exception("Unable to read data from mysql database")
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
        .agg(sum("work_durations") as "totalhours")
      summedDF
    } catch {
      case ex: Exception =>
        logger.error(ex.printStackTrace())
        throw new Exception("Unable to sum daily userlog time")
    }
  }
}
