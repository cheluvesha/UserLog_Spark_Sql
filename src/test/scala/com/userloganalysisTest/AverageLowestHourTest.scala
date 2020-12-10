package com.userloganalysisTest

import java.sql.Timestamp

import com.userloganalysis.{AverageLowestHour, UtilityClass}
import org.apache.spark.sql.functions.{col, to_timestamp}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class AverageLowestHourTest extends FunSuite with BeforeAndAfterAll {
  var spark: SparkSession = _
  var averageLowestHour: AverageLowestHour = _
  val data = Seq(("2019-05-21 06:05:02", "xyzname", 10.0, 41.00))
  val csvFilePath = "./Data/CpuLogData2019-09-16.csv"
  val wrongCSV = "./Data/testCSV.csv"
  val column = Seq("datetime", "username", "keyboard", "mouse")
  var testDF: DataFrame = _
  val resultDFContent: Array[Row] = Array(
    Row("xyzname", 10.0, 41.00, "2019-05-21", "06:05:02")
  )
  override def beforeAll(): Unit = {
    spark = UtilityClass.createSparkSessionObj("Average lowest hour Test App")
    averageLowestHour = new AverageLowestHour(spark)
    val spark1 = spark
    import spark1.implicits._
    testDF = data.toDF(column: _*)
    testDF.withColumn("datetime", to_timestamp(col("datetime")))
  }
  test("DataFrameCountMustBeGreaterThanZero") {
    val userlogRowDF = averageLowestHour.readDataFromMySqlForDataFrame()
    assert(userlogRowDF.count() > 0)
  }

  test("DateFrameTypeMustToTheGivenType") {
    val rowList = averageLowestHour.readDataFromMySqlForDataFrame().take(1)
    rowList.foreach { row =>
      assert(
        row.get(0).isInstanceOf[Timestamp],
        "zero column should be Timestamp"
      )
      assert(
        row.get(1).isInstanceOf[String],
        "First column should be String"
      )
    }
  }
  test("checkTheDataOfDataFrame") {
    val rowData = averageLowestHour.readDataFromMySqlForDataFrame().take(1)
    var name: Any = null
    var keyboard: Any = null
    var mouse: Any = null
    rowData.foreach { row =>
      name = row.get(1)
      keyboard = row.get(2)
      mouse = row.get(3)
    }
    assert(keyboard === 0.0)
    assert(mouse === 0.0)
    assert(name === "rahilstar11@gmail.com")
  }
  test(
    "givenDataFrameAsInputMustPerformSplitTimeAndDateShouldEqualToExceptedDataFrame"
  ) {
    val userlogDF = averageLowestHour.splitAndCreateUserlogDF(testDF)

    assert(resultDFContent === userlogDF.collect())
  }
  override def afterAll(): Unit = {
    spark.stop()
  }
}
