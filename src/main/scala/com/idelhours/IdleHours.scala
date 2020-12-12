package com.idelhours

import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, collect_list, substring}
import org.apache.spark.sql.{DataFrame, SparkSession}

class IdleHours(sparkSession: SparkSession) {

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
    * Creates View table to select keyboard and mouse column with condition
    * @param userlogDF DataFrame
    * @return DataFrame
    */
  def analyzeKeyboardAndMouseDataFromTable(userlogDF: DataFrame): DataFrame = {
    userlogDF.createTempView("userlog_KMAnalyze")
    val keyMouseAnalyzedDF = sparkSession.sql(
      """SELECT username,datetime,CASE WHEN keyboard > 0.0 OR mouse > 0.0 THEN 1 ELSE 0 END as KMAnalyzed FROM userlog_KMAnalyze"""
    )
    keyMouseAnalyzedDF
  }

  def groupConcatTheData(keyMouseAnalyzedDF: DataFrame): DataFrame = {
    val selectDataDF = keyMouseAnalyzedDF.select(
      col("username"),
      substring(col("datetime"), 1, 10) as "dates",
      col("KMAnalyzed")
    )
    val groupConcatDF = selectDataDF
      .groupBy("username", "dates")
      .agg(
        collect_list("KMAnalyzed")
          .as("guessIdleHr")
      )
    /*groupConcatDF.take(1).foreach { row =>
      val arr = row.get(2).asInstanceOf[Seq[Int]]
      println(arr)
    } */
    groupConcatDF
  }
}
