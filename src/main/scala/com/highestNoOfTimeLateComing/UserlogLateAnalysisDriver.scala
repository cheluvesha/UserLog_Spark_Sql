package com.highestNoOfTimeLateComing

import com.Utility.UtilityClass

/***
  * Driver class performs function class to perform late analysis on userlog
  */
object UserlogLateAnalysisDriver extends App {
  val hdfsFilePath = "hdfs://localhost:54310/userlogs_header/*.csv"
  val sparkSession = UtilityClass.createSparkSessionObj("UserlogLateAnalysis")
  val loginTime = "09:00:00"
  val userlogLateAnalysis = new UserlogLateAnalysis(sparkSession)
  val userlogsDF = userlogLateAnalysis.readFilesFromHDFS(hdfsFilePath)
  userlogsDF.show()
  userlogsDF.printSchema()
  val selectedUserlogDF = userlogLateAnalysis.selectRequiredColumn(userlogsDF)
  selectedUserlogDF.printSchema()
  selectedUserlogDF.show()
  val loginTimeDF = userlogLateAnalysis.findLoginTimeForUsers(selectedUserlogDF)
  loginTimeDF.printSchema()
  loginTimeDF.show()
  val appendLoginTimeDF =
    userlogLateAnalysis.appendActualLoginTimeToDate(loginTimeDF, loginTime)
  appendLoginTimeDF.printSchema()
  appendLoginTimeDF.show()

}
