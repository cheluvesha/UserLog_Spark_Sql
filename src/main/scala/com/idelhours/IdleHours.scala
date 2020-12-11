package com.idelhours

import org.apache.log4j.Logger
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
      """select username,datetime,case when keyboard > 0.0 or mouse > 0.0 then 1 else 0 end as KM_analyzed from userlog_KMAnalyze"""
    )
    keyMouseAnalyzedDF
  }
  def groupConcatTheData(keyMouseAnalyzedDF: DataFrame) = ???
}
