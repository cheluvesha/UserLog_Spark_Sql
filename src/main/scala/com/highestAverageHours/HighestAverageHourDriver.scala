package com.highestAverageHours

import com.Utility.UtilityClass
import org.apache.spark.sql.SparkSession

/***
  * Driver Class calls respective functions to perform analysis and computation on userlogs
  */
object HighestAverageHourDriver extends App {
  try {
    val dbName = System.getenv("DB_NAME")
    val readTable = System.getenv("TABLE")
    val username = System.getenv("MYSQL_UN")
    val password = System.getenv("MYSQL_PW")
    val url = System.getenv("URL")
    val tableName = "userlog_AvgLowHours"
    val xmlFilePath = "./HighHrs/UserAvgHighHours.xml"
    val jsonFilePath = "./HighHrs/UserAvgHighHours.json"
    val idleHoursTable = "userlog_idlehours"
    val days = 6
    val sparkSession: SparkSession =
      UtilityClass.createSparkSessionObj("AverageLowestHours")
    val highestAverageHour = new HighestAverageHour(sparkSession)
    val userlogsDataFrame = highestAverageHour.readDataFromMySqlForDataFrame(
      dbName,
      readTable,
      username,
      password,
      url
    )
    userlogsDataFrame.printSchema()
    userlogsDataFrame.show()
    val selectColFromDF =
      highestAverageHour.selectRequiredColumn(userlogsDataFrame)
    val splittedDataFrame =
      highestAverageHour.splitAndCreateUserlogDF(userlogsDataFrame)
    splittedDataFrame.printSchema()
    splittedDataFrame.show()
    val minTimeDF =
      highestAverageHour.findMinAndMaxTimeForUsers(splittedDataFrame)
    minTimeDF.printSchema()
    minTimeDF.show()
    val dailyHourDF = highestAverageHour.calculateDailyHour(minTimeDF)
    dailyHourDF.printSchema()
    dailyHourDF.show()
    val sumByUserDF = highestAverageHour.sumDailyHourWRTUser(dailyHourDF)
    sumByUserDF.show()
    val idleHoursDF = highestAverageHour.readDataFromMySqlForDataFrame(
      dbName,
      idleHoursTable,
      username,
      password,
      url
    )
    val idleHoursBV = highestAverageHour.createIdleHoursBroadCast(idleHoursDF)
    val overAllAverageHourDF =
      highestAverageHour.findAverageHourByDifferencingIdleHours(
        sumByUserDF,
        idleHoursBV,
        days
      )
    overAllAverageHourDF.show()
    val highestAverageHourDF =
      highestAverageHour.sortByHighestAverageHours(overAllAverageHourDF)
    highestAverageHourDF.show()
  } catch {
    case ex: Exception =>
      println(ex.printStackTrace())
  }
}