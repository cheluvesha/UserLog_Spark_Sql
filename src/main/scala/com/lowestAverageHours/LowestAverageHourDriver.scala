package com.lowestAverageHours

import com.Utility.UtilityClass
import org.apache.spark.sql.SparkSession

/***
  * Driver Class calls respective functions to perform analysis and computation on userlogs
  */
object LowestAverageHourDriver extends App {
  try {
    val dbName = System.getenv("DB_NAME")
    val readTable = System.getenv("TABLE")
    val username = System.getenv("MYSQL_UN")
    val password = System.getenv("MYSQL_PW")
    val url = System.getenv("URL")
    val tableName = "userlog_AvgLowHours"
    val xmlFilePath = "./idleHrs/UserAvgLowHours.xml"
    val jsonFilePath = "./idleHrs/UserAvgLowHours.json"
    val sparkSession: SparkSession =
      UtilityClass.createSparkSessionObj("AverageLowestHours")
    val averageLowestHour = new LowestAverageHour(sparkSession)
    val userlogsDataFrame = averageLowestHour.readDataFromMySqlForDataFrame(
      dbName,
      readTable,
      username,
      password,
      url
    )
    userlogsDataFrame.printSchema()
    userlogsDataFrame.show()
    val splittedDataFrame =
      averageLowestHour.splitAndCreateUserlogDF(userlogsDataFrame)
    splittedDataFrame.printSchema()
    splittedDataFrame.show()
    val minTimeDF =
      averageLowestHour.findMinAndMaxTimeForUsers(splittedDataFrame)
    minTimeDF.printSchema()
    minTimeDF.show()
    val dailyHourDF = averageLowestHour.calculateDailyHour(minTimeDF)
    dailyHourDF.printSchema()
    dailyHourDF.show()
    val sumByUserDF = averageLowestHour.sumDailyHourWRTUser(dailyHourDF)
    sumByUserDF.show()
  } catch {
    case ex: Exception =>
      println(ex.printStackTrace())
  }
}
