package com.highestNoOfTimeLateComing

import com.Utility.{UtilityClass, WriteDataToSource}
import org.apache.spark.sql.DataFrame

/***
  * Driver class performs function class to perform late analysis on userlog
  */
object UserlogLateAnalysisDriver extends App {
  val hdfsFilePath = System.getenv("hdfsFilePath")
  val dbName = System.getenv("DB_NAME")
  val lateComerTable = "userlog_lateComers"
  val noOfLeavesTable = "userlog_noOfLeaves"
  val averageLateHoursTable = "userlog_averageLateHours"
  val lateComerXmlFilePath = "./late/lateComers.xml"
  val lateComerJsonFilePath = "./late/lateComers.json"
  val noOfLeavesXmlFilePath = "./leaves/noOfLeaves.xml"
  val noOfLeavesJsonFilePath = "./leaves/noOfLeaves.json"
  val averageLateHoursXml = "./averageLateHours/lateHour.xml"
  val averageLateHoursJson = "./averageLateHours/lateHour.json"
  val sparkSession = UtilityClass.createSparkSessionObj("UserlogLateAnalysis")
  val loginTime = "09:00:00"
  val userlogLateAnalysis = new UserlogLateAnalysis(sparkSession)
  var selectedUserlogDF: DataFrame = _
  var appendLoginTimeDF: DataFrame = _
  var noOfTimesLateComingDF: DataFrame = _
  var highestNoOfLateComersDF: DataFrame = _
  var totalNumberOfLeavesDF: DataFrame = _
  var averageLateHoursDF: DataFrame = _

  /***
    * Calls the respective functions to find the number of late coming users
    */
  def findNoOfLateComing(): Unit = {
    val userlogsDF = userlogLateAnalysis.readFilesFromHDFS(hdfsFilePath)
    userlogsDF.show()
    userlogsDF.printSchema()
    selectedUserlogDF = userlogLateAnalysis.selectRequiredColumn(userlogsDF)
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

  /***
    * Calls the respective functions to find the average late coming users
    */
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
    averageLateHoursDF = userlogLateAnalysis.findAverageLateHours(
      totalLateHoursDF,
      broadcastNoOfTimesLate
    )
    averageLateHoursDF.printSchema()
    averageLateHoursDF.show()
  }

  /***
    * Calls the respective functions to find the total number of leaves taken by users
    */
  def findTotalNumberOfLeaves(): Unit = {
    val noOfWorkingDays =
      userlogLateAnalysis.findNoOfWorkingDays(selectedUserlogDF)
    totalNumberOfLeavesDF = userlogLateAnalysis.findUsersTotalNumberOfLeaves(
      noOfWorkingDays,
      selectedUserlogDF
    )
    totalNumberOfLeavesDF.show()
  }

  /***
    * Writes the DataFrame to Mysql table
    * @param dataFrame DataFrame
    * @param dbName String
    * @param tableName String
    */
  def writeDataToMysqlTable(
      dataFrame: DataFrame,
      dbName: String,
      tableName: String
  ): Unit = {
    val mySqlStatus = WriteDataToSource.writeDataFrameToMysql(
      dataFrame,
      dbName,
      tableName
    )
    if (mySqlStatus) {
      println("Successfully Data written into Mysql table")
    } else {
      println("Unable to write Data into Mysql table")
    }
  }

  /***
    * Writes the DataFrame to Json file
    * @param analyzedDataFrame DataFrame
    * @param jsonFilePath String
    */
  def writeDataToJsonFile(
      analyzedDataFrame: DataFrame,
      jsonFilePath: String
  ): Unit = {
    val jsonStatus = WriteDataToSource.writeDataFrameIntoJsonFormat(
      analyzedDataFrame,
      jsonFilePath
    )
    if (jsonStatus) {
      println("Successfully Data written into json format")
    } else {

      println("Unable to write Data into json format")
    }
  }

  /***
    * Writes the DataFrame to XMl file
    * @param analyzedDataFrame DataFrame
    * @param xmlFilePath String
    */
  def writeDataToXml(
      analyzedDataFrame: DataFrame,
      xmlFilePath: String
  ): Unit = {
    val xmlStatus =
      WriteDataToSource.writeDataFrameInToXMLFormat(
        analyzedDataFrame,
        xmlFilePath
      )
    if (xmlStatus) {
      println("Successfully Data written into Xml file")
    } else {
      println("Unable to write Data into Xml file")
    }
  }
  findNoOfLateComing()
  findAverageLateHours()
  findTotalNumberOfLeaves()
  writeDataToMysqlTable(highestNoOfLateComersDF, dbName, lateComerTable)
  writeDataToMysqlTable(averageLateHoursDF, dbName, averageLateHoursTable)
  writeDataToMysqlTable(totalNumberOfLeavesDF, dbName, noOfLeavesTable)
  writeDataToJsonFile(highestNoOfLateComersDF, lateComerJsonFilePath)
  writeDataToJsonFile(averageLateHoursDF, averageLateHoursJson)
  writeDataToJsonFile(totalNumberOfLeavesDF, noOfLeavesJsonFilePath)
  writeDataToXml(highestNoOfLateComersDF, lateComerXmlFilePath)
  writeDataToXml(averageLateHoursDF, averageLateHoursXml)
  writeDataToXml(totalNumberOfLeavesDF, noOfLeavesXmlFilePath)
}
