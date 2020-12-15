package com.highestNoOfTimeLateComing

import com.Utility.{UtilityClass, WriteDataToSource}

/***
  * Driver class performs function class to perform late analysis on userlog
  */
object UserlogLateAnalysisDriver extends App {
  try {
    val hdfsFilePath = System.getenv("hdfsFilePath")
    val dbName = System.getenv("DB_NAME")
    val tableName = "userlog_lateComers"
    val xmlFilePath = "./late/lateComers.xml"
    val jsonFilePath = "./late/lateComers.json"
    val sparkSession = UtilityClass.createSparkSessionObj("UserlogLateAnalysis")
    val loginTime = "09:00:00"
    val userlogLateAnalysis = new UserlogLateAnalysis(sparkSession)
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
    val appendLoginTimeDF =
      userlogLateAnalysis.appendActualLoginTimeToDate(loginTimeDF, loginTime)
    appendLoginTimeDF.printSchema()
    appendLoginTimeDF.show()
    val noOfTimesLateComingDF =
      userlogLateAnalysis.findLateComing(appendLoginTimeDF)
    noOfTimesLateComingDF.printSchema()
    noOfTimesLateComingDF.show()
    val highestNoOfLateComersDF =
      userlogLateAnalysis.findHighestNoOfTimeLateComers(noOfTimesLateComingDF)
    highestNoOfLateComersDF.printSchema()
    highestNoOfLateComersDF.show(false)
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
  } catch {
    case ex: Exception =>
      println(ex.printStackTrace())
  }
}
