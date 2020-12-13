package com.idelhours

import com.Utility.{UtilityClass, WriteDataToSource}

/***
  * Driver Class Performs IdleHour Analysis
  */
object IdleHoursDriver extends App {
  try {
    val dbName = System.getenv("DB_NAME")
    val readTable = System.getenv("TABLE")
    val username = System.getenv("MYSQL_UN")
    val password = System.getenv("MYSQL_PW")
    val url = System.getenv("URL")
    val tableName = "userlog_idlehours"
    val xmlFilePath = "./idleHrs/UserIdleHours.xml"
    val jsonFilePath = "./idleHrs/UserIdleHours.json"
    val sparkSession = UtilityClass.createSparkSessionObj("IdleHours")
    val idleHours = new IdleHours(sparkSession)
    val userlogsDF =
      idleHours.readDataFromMySqlForDataFrame(
        dbName,
        readTable,
        username,
        password,
        url
      )
    userlogsDF.printSchema()
    userlogsDF.show()
    val keyMouseAnalyzedDF =
      idleHours.analyzeKeyboardAndMouseDataFromTable(userlogsDF)
    keyMouseAnalyzedDF.printSchema()
    keyMouseAnalyzedDF.show()
    val groupConcatDF = idleHours.groupConcatTheData(keyMouseAnalyzedDF)
    groupConcatDF.printSchema()
    groupConcatDF.show()
    val usersIdleHoursData = idleHours.findIdleHour(groupConcatDF)
    val usersIdleHoursDF = idleHours.createDataFrame(usersIdleHoursData)
    usersIdleHoursDF.show()
    val highToLowIdleHrDF = idleHours.findHighestIdleHour(usersIdleHoursDF)
    highToLowIdleHrDF.show()
    val mySqlStatus = WriteDataToSource.writeDataFrameToMysql(
      highToLowIdleHrDF,
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
        highToLowIdleHrDF,
        xmlFilePath
      )
    if (xmlStatus) {
      println("Successfully Data written into Xml file")
    } else {
      println("Unable to write Data into Xml file")
    }
    val jsonStatus = WriteDataToSource.writeDataFrameIntoJsonFormat(
      highToLowIdleHrDF,
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
