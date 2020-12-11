package com.userloganalysis

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

class AverageLowestHour(sparkSession: SparkSession) {

  sparkSession.sparkContext.setLogLevel("OFF")
  lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def readDataFromMySqlForDataFrame(): DataFrame = {
    logger.info("Read operation started to create dataframe from mysql")
    val userlogReadDF = sparkSession.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/fellowship")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", "userlogs")
      .option("user", "user")
      .option("password", "user00")
      .load()
    userlogReadDF.printSchema()
    logger.info("Read operation ended")
    val userlogDF =
      userlogReadDF.select("datetime", "username", "keyboard", "mouse")
    userlogDF.show(false)
    userlogDF
  }

  def splitAndCreateUserlogDF(userDataFrame: DataFrame): DataFrame = {
    logger.info("started splitAndCreateUserlogDF")
    val splitDF = userDataFrame.select(
      col("datetime"),
      to_date(col("datetime"), "yyyy-MM-dd") as "dates",
      col("username")
    )
    logger.info("table is created")
    splitDF.printSchema()
    splitDF.show()
    splitDF
  }

  def findMinAndMaxTimeForUsers(userLogsDataFrame: DataFrame): DataFrame = {
    logger.info("Entered into function to find min and max time")
    val minAndMaxDF = userLogsDataFrame
      .groupBy("username", "dates")
      .agg(min("datetime") as "login", max("datetime") as "logout")
    minAndMaxDF.printSchema()
    minAndMaxDF.show()
    minAndMaxDF
  }
  def calculateDailyHour(minTimeDF: DataFrame): DataFrame = {
    val dailyHourDF = minTimeDF.select(
      col("username"),
      col("dates"),
      (unix_timestamp(col("logout")) - unix_timestamp(
        col("login")
      )) / 3600 as "work_durations"
    )
    dailyHourDF.show()
    dailyHourDF.printSchema()
    dailyHourDF
  }
  def sumDailyHourWRTUser(dailyHourDF: DataFrame): DataFrame = {
    val summedDF = dailyHourDF
      .groupBy("username")
      .agg(sum("work_durations") as "totalhours")
    summedDF.show(false)
    summedDF
  }
}
