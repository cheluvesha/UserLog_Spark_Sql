package com.idelhours

import com.Utility.{UtilityClass, WriteDataToSource}
import org.apache.spark.sql.DataFrame

/***
  * Driver Class Performs IdleHour Analysis
  */
object IdleHoursDriver extends App {
  var highToLowIdleHrDF: DataFrame = _
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

  /***
    * Finds the Idle hours by calling IdleHours class methods
    */
  def findIdleHours(): Unit = {
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
    highToLowIdleHrDF = idleHours.findHighestIdleHour(usersIdleHoursDF)
    highToLowIdleHrDF.show()
  }

  /***
    * Writes Data to Source by calling WriteToSource class methods
   **/
  def writeDataToSource(): Unit = {
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
  }
  findIdleHours()
  writeDataToSource()
  sparkSession.stop()
}
