package com.idelhours

import java.sql.SQLSyntaxErrorException
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable

class IdleHours(sparkSession: SparkSession) {

  lazy val logger: Logger = Logger.getLogger(getClass.getName)

  /***
    * Reads Data From Mysql Database and Creates DataFrame
    * @return DataFrame
    */
  def readDataFromMySqlForDataFrame(
      dbName: String,
      tableName: String,
      username: String,
      password: String,
      url: String
  ): DataFrame = {
    logger.info("Read operation started to create dataframe from mysql")
    try {
      val userlogReadDF = sparkSession.read
        .format("jdbc")
        .option("url", url + dbName)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("dbtable", tableName)
        .option("user", username)
        .option("password", password)
        .load()
      logger.info("Read operation ended")
      val userlogDF =
        userlogReadDF.select("datetime", "username", "keyboard", "mouse")
      userlogDF
    } catch {
      case sqlSyntaxErrorException: SQLSyntaxErrorException =>
        logger.error(sqlSyntaxErrorException.printStackTrace())
        throw new Exception(
          "Fault Database Credentials, Please Provide Proper DB and Table configurations"
        )
      case nullPointerException: NullPointerException =>
        logger.error(nullPointerException.printStackTrace())
        throw new Exception(
          "Parameters Are Null,Please Check The Passed Parameters"
        )
      case sparkAnalysisException: org.apache.spark.sql.AnalysisException =>
        logger.error(sparkAnalysisException.printStackTrace())
        throw new Exception(
          "Please Provide Existing Column Name to Select Column From DataFrame"
        )
      case ex: Exception =>
        logger.error(ex.printStackTrace())
        throw new Exception("Unable To Read Data From Mysql Database")
    }
  }

  /***
    * Creates View table to select keyboard and mouse column with condition
    * @param userlogDF DataFrame
    * @return DataFrame
    */
  def analyzeKeyboardAndMouseDataFromTable(userlogDF: DataFrame): DataFrame = {
    logger.info("Executing analyzeKeyboardAndMouseDataFromTable")
    try {
      userlogDF.createTempView("userlog_KMAnalyze")
      val keyMouseAnalyzedDF = sparkSession.sql(
        """SELECT username,datetime,CASE WHEN keyboard > 0.0 OR mouse > 0.0 THEN 1 ELSE 0 END AS KMAnalyzed FROM userlog_KMAnalyze"""
      )
      keyMouseAnalyzedDF
    } catch {
      case sqlException: org.apache.spark.sql.AnalysisException =>
        logger.error(sqlException.printStackTrace())
        throw new Exception("SQL Syntax Error Please Check The Syntax")
      case ex: Exception =>
        logger.error(ex.printStackTrace())
        throw new Exception("Unable To Analyze Keyboard and Mouse Data")
    }
  }

  /***
    * Performs the Sql query operation get Date from Timestamp and folds
    * column data to list for each user for each day
    * @param keyMouseAnalyzedDF DataFrame
    * @return DataFrame
    */
  def groupConcatTheData(keyMouseAnalyzedDF: DataFrame): DataFrame = {
    logger.info("Executing groupConcatTheData")
    try {
      keyMouseAnalyzedDF.createTempView("KMAnalyzedTable")
      val groupConcatDF = sparkSession.sql(
        """SELECT username,SUBSTR(datetime,1,10) as dates,collect_list(KMAnalyzed) AS guessIdleHr FROM KMAnalyzedTable GROUP BY username, dates"""
      )
      groupConcatDF
    } catch {
      case sqlException: org.apache.spark.sql.AnalysisException =>
        logger.error(sqlException.printStackTrace())
        throw new Exception("SQL Syntax Error Please Check The Syntax")
      case ex: Exception =>
        logger.error(ex.printStackTrace())
        throw new Exception("Unable To Analyze Keyboard and Mouse Data")
    }
  }

  /***
    * loops through array to find 0 digit sequences
    * @param arrayMins Array[Int]
    * @return Int
    */
  def checkForZeros(arrayMins: Array[Int]): Int = {
    val one = 1
    val zero = 0
    val idleLimit = 6
    var zeroCount = one
    var countedZero = zero
    var mins = zero
    try {
      while (mins < arrayMins.length - one) {
        if ((arrayMins(mins) == zero) && (arrayMins(mins + one) == zero)) {
          zeroCount += one
        } else {
          if (zeroCount >= idleLimit) {
            countedZero += zeroCount
            zeroCount = one
          } else {
            zeroCount = one
          }
        }
        mins += one
      }
      if (zeroCount >= idleLimit) {
        countedZero += zeroCount
      }
      countedZero
    } catch {
      case ex: Exception =>
        logger.error(ex.printStackTrace())
        throw new Exception("Unable To Find Sequence Of Zeros")
    }
  }

  /***
    * calculates time on the basis of Array elements data
    * @param guessIdleHrArray Array[Int]
    * @return Double
    */
  def evaluateTime(guessIdleHrArray: Array[Int]): Double = {
    try {
      val minutes = 5
      val zeroCounted = checkForZeros(guessIdleHrArray)
      val totalMins = zeroCounted * minutes
      val hour: Double = totalMins.toDouble / 60
      val roundedHours =
        BigDecimal(hour).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
      roundedHours
    } catch {
      case ex: Exception =>
        println(ex.printStackTrace())
        throw new Exception("Unable to evaluate time")
    }
  }

  /***
    * Finds idle hour and stores it into LinkedHashMap
    * @param guessIdleHrsDF DataFrame
    * @return mutable.LinkedHashMap[String, Double]
    */
  def findIdleHour(
      guessIdleHrsDF: DataFrame
  ): mutable.LinkedHashMap[String, Double] = {
    logger.info("Executing findIdleHour")
    val collectUserData = guessIdleHrsDF.collect()
    val userLogData = new mutable.LinkedHashMap[String, Double]
    collectUserData.foreach { rowData =>
      val username = rowData.get(0).asInstanceOf[String]
      val guessIdleHrArray = rowData.get(2).asInstanceOf[Seq[Int]].toArray
      val idleHours = evaluateTime(guessIdleHrArray)
      if (!userLogData.contains(username)) {
        userLogData.put(username, idleHours)
      } else {
        val hour = userLogData.getOrElse(username, 0.0)
        val overAllHours = idleHours + hour
        userLogData.put(username, overAllHours)
      }
    }
    userLogData
  }

  /***
    * Creates DataFrame from Map
    * @param usersIdleHoursData DataFrame
    * @return DataFrame
    */
  def createDataFrame(
      usersIdleHoursData: mutable.LinkedHashMap[String, Double]
  ): DataFrame = {
    logger.info("Executing CreateDataFrame")
    import sparkSession.implicits._
    val usersSchema = Seq("username", "idlehours")
    val idleHoursDF = usersIdleHoursData.toSeq.toDF(usersSchema: _*)
    idleHoursDF
  }

  /***
    * Sql query to order the idle hour data from highest order
    * @param usersIdleHoursDF DataFrame
    * @return DataFrame
    */
  def findHighestIdleHour(usersIdleHoursDF: DataFrame): DataFrame = {
    logger.info("Executing findHighestIdleHour")
    try {
      usersIdleHoursDF.createTempView("usersIdleHours")
      val highToLowIdleHrDF = sparkSession.sql(
        """SELECT username, idlehours FROM usersIdleHours ORDER BY idlehours DESC"""
      )
      highToLowIdleHrDF
    } catch {
      case sqlAnalysisException: org.apache.spark.sql.AnalysisException =>
        logger.error(sqlAnalysisException.printStackTrace())
        throw new Exception("SQL Syntax Error, Please Check the Syntax")
    }
  }
}
