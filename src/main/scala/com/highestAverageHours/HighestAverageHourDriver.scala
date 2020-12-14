package com.highestAverageHours

import com.Utility.{UtilityClass, WriteDataToSource}
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
    val tableName = "userlog_HighAvgHours"
    val xmlFilePath = "./HighHrs/UserAvgHighHours.xml"
    val jsonFilePath = "./HighHrs/UserAvgHighHours.json"
    val idleHoursTable = "userlog_idlehours"
    val days = 6
    val sparkSession: SparkSession =
      UtilityClass.createSparkSessionObj("HighestAverageHours")
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
      highestAverageHour.splitAndCreateUserlogDF(selectColFromDF)
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
      highestAverageHour.findHourByDifferencingIdleHours(
        sumByUserDF,
        idleHoursBV,
        days
      )
    overAllAverageHourDF.show()
    val highestAverageHourDF =
      highestAverageHour.sortByHighestAverageHours(overAllAverageHourDF)
    highestAverageHourDF.show()
    val mySqlStatus = WriteDataToSource.writeDataFrameToMysql(
      highestAverageHourDF,
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
        highestAverageHourDF,
        xmlFilePath
      )
    if (xmlStatus) {
      println("Successfully Data written into Xml file")
    } else {
      println("Unable to write Data into Xml file")
    }
    val jsonStatus = WriteDataToSource.writeDataFrameIntoJsonFormat(
      highestAverageHourDF,
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
