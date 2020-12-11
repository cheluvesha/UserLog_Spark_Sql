package com.userloganalysisTest

import java.sql.Timestamp
import com.userloganalysis.{AverageLowestHour, UtilityClass}
import org.apache.spark.sql.functions.{col, to_timestamp}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.mutable

class AverageLowestHourTest extends FunSuite with BeforeAndAfterAll {
  var spark: SparkSession = _
  var averageLowestHour: AverageLowestHour = _
  val data = Seq(
    ("2019-05-21 06:05:02", "xyzname", 10.0, 41.00),
    ("2019-05-21 15:05:02", "xyzname", 10.0, 41.00),
    ("2019-05-21 06:05:02", "testname", 30.0, 45.00)
  )
  val column = Seq("datetime", "username", "keyboard", "mouse")
  var splitTestDF: DataFrame = _
  var minMaxTestDF: DataFrame = _
  val resultDFContent = Seq("2019-05-21 06:05:02", "2019-05-21", "xyzname")
  val splitSchemaDF =
    "StructType(StructField(datetime,StringType,true), StructField(dates,DateType,true), StructField(username,StringType,true))"
  val minMaxSchema =
    "StructType(StructField(username,StringType,true), StructField(dates,DateType,true), StructField(login,StringType,true), StructField(logout,StringType,true))"
  val dailyHourData =
    Seq("xyzname", "2019-05-21", "2019-05-21 06:05:02", "2019-05-21 15:05:02")
  val hourTestData = Seq("xyzname", "2019-05-21", 9.0)
  var dailyHourDF: DataFrame = _
  var hourTestDF: DataFrame = _
  override def beforeAll(): Unit = {
    spark = UtilityClass.createSparkSessionObj("Average lowest hour Test App")
    averageLowestHour = new AverageLowestHour(spark)
    val spark1 = spark
    import spark1.implicits._
    splitTestDF = data.toDF(column: _*)
    splitTestDF.withColumn("datetime", to_timestamp(col("datetime")))
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
    var time: Any = null
    var name: Any = null
    var keyboard: Any = null
    var mouse: Any = null
    rowData.foreach { row =>
      time = row.get(0)
      name = row.get(1)
      keyboard = row.get(2)
      mouse = row.get(3)
    }
    assert(time.toString === "2019-09-16 12:55:03.0")
    assert(keyboard === 0.0)
    assert(mouse === 0.0)
    assert(name === "rahilstar11@gmail.com")

  }
  test(
    "givenDataFrameAsInputMustPerformSplitTimeAndDateShouldEqualToExceptedDataFrame"
  ) {
    minMaxTestDF = averageLowestHour.splitAndCreateUserlogDF(splitTestDF)
    assert(minMaxTestDF.schema.toString() === splitSchemaDF)
    minMaxTestDF.take(1).foreach { row =>
      assert(row.get(0).toString === resultDFContent.head)
      assert(row.get(1).toString === resultDFContent(1))
      assert(row.get(2) === resultDFContent(2))
    }
  }
  test("givenMinMaxTestDFAsInputToFindMinAndMaxAndResultMustEqualToExpected") {
    dailyHourDF = averageLowestHour.findMinAndMaxTimeForUsers(minMaxTestDF)
    assert(dailyHourDF.schema.toString() === minMaxSchema)
    dailyHourDF.take(1).foreach { row =>
      assert(row.get(0) === dailyHourData.head)
      assert(row.get(1).toString === dailyHourData(1))
      assert(row.get(2) === dailyHourData(2))
      assert(row.get(3) === dailyHourData.last)
    }
  }
  test("givenInputDFAsInputToCalculateDailyHourAndResultMustEqualToExpected") {
    hourTestDF = averageLowestHour.calculateDailyHour(dailyHourDF)
    hourTestDF.take(1).foreach { rowData =>
      assert(rowData.get(0) === hourTestData.head)
      assert(rowData.get(1).toString === hourTestData(1))
      assert(rowData.get(2) === hourTestData.last)
    }
  }
  test("givenInputDFAsInputToSumDailyHoursAndResultMustEqualToExcepted") {
    val sumHourDF = averageLowestHour.sumDailyHourWRTUser(hourTestDF)
    val userAndHourMap = new mutable.HashMap[String, String]()
    sumHourDF.collect().foreach { user =>
      userAndHourMap.put(user.get(0).toString, user.get(1).toString)
    }
    assert(userAndHourMap("xyzname") == "9.0")
    assert(userAndHourMap("testname") == "0.0")
  }
  override def afterAll(): Unit = {
    spark.stop()
  }
}
