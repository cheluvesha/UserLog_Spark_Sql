package com.userloganalysis

import org.apache.spark.sql.SparkSession

object AverageLowestHourDriver extends App {
  val sparkSession: SparkSession =
    UtilityClass.createSparkSessionObj("AverageLowestHours")
  val averageLowestHour = new AverageLowestHour(sparkSession)
  val userDataFrame = averageLowestHour.readDataFromMySqlForDataFrame()
  val splitDateAndTimeDF =
    averageLowestHour.splitAndCreateUserlogDF(userDataFrame)
}
