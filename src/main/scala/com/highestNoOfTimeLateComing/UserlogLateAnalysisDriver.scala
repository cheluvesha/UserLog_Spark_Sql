package com.highestNoOfTimeLateComing

import com.Utility.{UtilityClass, WriteDataToSource}
import org.apache.spark.sql.DataFrame

/***
  * Driver class performs function class to perform late analysis on userlog
  */
object UserlogLateAnalysisDriver extends App {
  val hdfsFilePath = System.getenv("hdfsFilePath")
  val dbName = System.getenv("DB_NAME")
  val tableName = "userlog_lateComers"
  val xmlFilePath = "./late/lateComers.xml"
  val jsonFilePath = "./late/lateComers.json"
  val sparkSession = UtilityClass.createSparkSessionObj("UserlogLateAnalysis")
  val loginTime = "09:00:00"
  val userlogLateAnalysis = new UserlogLateAnalysis(sparkSession)
  var appendLoginTimeDF: DataFrame = _
  var noOfTimesLateComingDF: DataFrame = _
  var highestNoOfLateComersDF: DataFrame = _
  def findNoOfLateComing(): Unit = {
    val userlogsDF = userlogLateAnalysis.readFilesFromHDFS(hdfsFilePath)
    userlogsDF.show()
    userlogsDF.printSchema()
    val selectedUserlogDF = userlogLateAnalysis.selectRequiredColumn(userlogsDF)
    selectedUserlogDF.printSchema()
    selectedUserlogDF.show()
    val loginTimeDF =
      userlogLateAnalysis.findLoginTimeForUsers(selectedUserlogDF)
    loginTimeDF.printSchema()
    loginTimeDF.show()
    appendLoginTimeDF =
      userlogLateAnalysis.appendActualLoginTimeToDate(loginTimeDF, loginTime)
    appendLoginTimeDF.printSchema()
    appendLoginTimeDF.show()
    noOfTimesLateComingDF =
      userlogLateAnalysis.findLateComing(appendLoginTimeDF)
    noOfTimesLateComingDF.printSchema()
    noOfTimesLateComingDF.show()
    highestNoOfLateComersDF =
      userlogLateAnalysis.findHighestNoOfTimeLateComers(noOfTimesLateComingDF)
    highestNoOfLateComersDF.printSchema()
    highestNoOfLateComersDF.show(false)
  }
  def findAverageLateHours(): Unit = {
    val usersLateHourDF =
      userlogLateAnalysis.findUsersLateHours(appendLoginTimeDF)
    usersLateHourDF.show()
    usersLateHourDF.printSchema()
    val totalLateHoursDF =
      userlogLateAnalysis.sumLateHoursForEachUsers(usersLateHourDF)
    totalLateHoursDF.show()
    totalLateHoursDF.printSchema()
    val broadcastNoOfTimesLate =
      userlogLateAnalysis.broadcastNoOfTimesLateComing(noOfTimesLateComingDF)
    val averageLateHoursDF =
      userlogLateAnalysis.findAverageLateHours(
        totalLateHoursDF,
        broadcastNoOfTimesLate
      )
    averageLateHoursDF.printSchema()
    averageLateHoursDF.show()
  }
  def writeHighestNoOfLateComingDataToSources(): Unit = {
    val mySqlStatus = WriteDataToSource.writeDataFrameToMysql(
      highestNoOfLateComersDF,
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
        highestNoOfLateComersDF,
        xmlFilePath
      )
    if (xmlStatus) {
      println("Successfully Data written into Xml file")
    } else {
      println("Unable to write Data into Xml file")
    }
    val jsonStatus = WriteDataToSource.writeDataFrameIntoJsonFormat(
      highestNoOfLateComersDF,
      jsonFilePath
    )
    if (jsonStatus) {
      println("Successfully Data written into json format")
    } else {
      println("Unable to write Data into json format")
    }
  }
}
