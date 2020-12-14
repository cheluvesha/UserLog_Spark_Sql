package com.lowestAverageHours

import com.Utility.{UtilityClass, WriteDataToSource}
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
    val tableName = "userlog_LowestAvgHours"
    val xmlFilePath = "./LowAVGHrs/UserAvgLowestHours.xml"
    val jsonFilePath = "./LowAVGHrs/UserAvgLowestHours.json"
    val idleHoursTable = "userlog_idlehours"
    val days = 6
    val sparkSession: SparkSession =
      UtilityClass.createSparkSessionObj("AverageLowestHours")
    val lowestAverageHour = new LowestAverageHour(sparkSession)
    val userlogsDataFrame = lowestAverageHour.readDataFromMySqlForDataFrame(
      dbName,
      readTable,
      username,
      password,
      url
    )
    userlogsDataFrame.printSchema()
    userlogsDataFrame.show()
    val selectColFromDF =
      lowestAverageHour.selectRequiredColumn(userlogsDataFrame)
    val splittedDataFrame =
      lowestAverageHour.splitAndCreateUserlogDF(selectColFromDF)
    splittedDataFrame.printSchema()
    splittedDataFrame.show()
    val minTimeDF =
      lowestAverageHour.findMinAndMaxTimeForUsers(splittedDataFrame)
    minTimeDF.printSchema()
    minTimeDF.show()
    val dailyHourDF = lowestAverageHour.calculateDailyHour(minTimeDF)
    dailyHourDF.printSchema()
    dailyHourDF.show()
    val sumByUserDF = lowestAverageHour.sumDailyHourWRTUser(dailyHourDF)
    sumByUserDF.show()
    val idleHoursDF = lowestAverageHour.readDataFromMySqlForDataFrame(
      dbName,
      idleHoursTable,
      username,
      password,
      url
    )
    val idleHoursBV = lowestAverageHour.createIdleHoursBroadCast(idleHoursDF)
    val overAllAverageHourDF =
      lowestAverageHour.findHourByDifferencingIdleHours(
        sumByUserDF,
        idleHoursBV,
        days
      )
    overAllAverageHourDF.show()
    val lowestAverageHourDF =
      lowestAverageHour.sortByLowestAverageHours(overAllAverageHourDF)
    lowestAverageHourDF.show()
    val mySqlStatus = WriteDataToSource.writeDataFrameToMysql(
      lowestAverageHourDF,
      dbName,
      tableName
    )
    if (mySqlStatus) {
      println("Successfully Data written into Mysql table")
    } else {
      println("Unable to write Data into Mysql table")
    }
    val xmlStatus =
      WriteDataToSource.writeDataFrameInToXMLFormat(
        lowestAverageHourDF,
        xmlFilePath
      )
    if (xmlStatus) {
      println("Successfully Data written into Xml file")
    } else {
      println("Unable to write Data into Xml file")
    }
    val jsonStatus = WriteDataToSource.writeDataFrameIntoJsonFormat(
      lowestAverageHourDF,
      jsonFilePath
    )
    if (jsonStatus) {
      println("Successfully Data written into json format")
    } else {
      println("Unable to write Data into json format")
    }
    sparkSession.stop()
  } catch {
    case ex: Exception =>
      println(ex.printStackTrace())
  }
}
