package com.userloganalysisTest

import com.userloganalysis.{AverageLowestHour, UtilityClass}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}

class AverageLowestHourTest extends FunSuite with BeforeAndAfter {
  var spark: SparkSession = _
  var avgLowHourObj: AverageLowestHour = _
  val csvFilePath = "./Data/CpuLogData2019-09-16.csv"
  val wrongCSV = "./Data/testCSV.csv"
  before {
    spark = UtilityClass.createSparkSessionObj("Average lowest hour Test App")
    avgLowHourObj = new AverageLowestHour
  }
  test(
    "DeclaredDataFrameCreationFromMySQLMustReadAndReturnDataFrameAndItHasToEqualToActual"
  ) {
    val userlogDF = avgLowHourObj.readDataFromMySqlForDataFrame()
    val testDF = spark.read
      .csv(csvFilePath)
    val result = DataFrameComparison.compareDataFrame(userlogDF, testDF)
    assert(result === true)
  }

  test(
    "DeclaredDataFrameCreationFromMySQLMustReadAndReturnDataFrameAndItHasToNotEqualToActual"
  ) {
    val userlogDF = avgLowHourObj.readDataFromMySqlForDataFrame()
    val testDF = spark.read
      .csv(wrongCSV)
    val result = DataFrameComparison.compareDataFrame(userlogDF, testDF)
    assert(result === false)
  }
}
