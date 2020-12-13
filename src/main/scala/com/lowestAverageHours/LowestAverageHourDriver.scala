package com.lowestAverageHours

import com.Utility.UtilityClass
import org.apache.spark.sql.SparkSession

/***
  * Driver Class calls respective functions to perform analysis and computation on userlogs
  */
object LowestAverageHourDriver extends App {

  val sparkSession: SparkSession =
    UtilityClass.createSparkSessionObj("AverageLowestHours")
  val averageLowestHour = new LowestAverageHour(sparkSession)
  val userlogsDataFrame = averageLowestHour.readDataFromMySqlForDataFrame()
  userlogsDataFrame.printSchema()
  userlogsDataFrame.show()
  val splittedDataFrame =
    averageLowestHour.splitAndCreateUserlogDF(userlogsDataFrame)
  splittedDataFrame.printSchema()
  splittedDataFrame.show()
  val minTimeDF = averageLowestHour.findMinAndMaxTimeForUsers(splittedDataFrame)
  minTimeDF.printSchema()
  minTimeDF.show()
  val dailyHourDF = averageLowestHour.calculateDailyHour(minTimeDF)
  dailyHourDF.printSchema()
  dailyHourDF.show()
  val sumByUserDF = averageLowestHour.sumDailyHourWRTUser(dailyHourDF)
  sumByUserDF.show()
}
