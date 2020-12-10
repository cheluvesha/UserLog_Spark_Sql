package com.userloganalysis

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

class AverageLowestHour {
  lazy val logger: Logger = Logger.getLogger(getClass.getName)
  val sparkSession: SparkSession =
    UtilityClass.createSparkSessionObj("AverageLowestHours")
  readDataFromMySqlForDataFrame()
  def readDataFromMySqlForDataFrame(): DataFrame = {
    logger.info("Read operation started to create dataframe from mysql")
    val userlogDF = sparkSession.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/fellowship")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "userlogs")
      .option("user", "user")
      .option("password", "user00")
      .load()
    userlogDF.show(10)
    logger.info("Read operation ended")
    userlogDF
  }
}
