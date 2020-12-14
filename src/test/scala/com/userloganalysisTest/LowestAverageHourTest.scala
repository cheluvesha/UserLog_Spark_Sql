package com.userloganalysisTest

import java.sql.Timestamp

import com.Utility.UtilityClass
import com.lowestAverageHours.LowestAverageHour
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.{col, to_timestamp}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import scala.collection.mutable

class LowestAverageHourTest extends FunSuite with BeforeAndAfterAll {
  var spark: SparkSession = _
  val dbName: String = System.getenv("DB_NAME")
  val readTable: String = System.getenv("TABLE")
  val username: String = System.getenv("MYSQL_UN")
  val password: String = System.getenv("MYSQL_PW")
  val url: String = System.getenv("URL")
  var lowestAverageHour: LowestAverageHour = _
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
  var readRowData: Array[Row] = _
  var sumHourDF: DataFrame = _
  val idleBVTestMap = Map("xyzname" -> 2.0, "testname" -> 0.0)
  var testBVIdleHour: Broadcast[collection.Map[String, Double]] = _
  var findHoursTestDF: DataFrame = _
  override def beforeAll(): Unit = {
    spark = UtilityClass.createSparkSessionObj("Average lowest hour Test App")
    lowestAverageHour = new LowestAverageHour(spark)
    val spark1 = spark
    import spark1.implicits._
    splitTestDF = data.toDF(column: _*)
    splitTestDF.withColumn("datetime", to_timestamp(col("datetime")))
    testBVIdleHour = spark.sparkContext.broadcast(idleBVTestMap)
  }

  test("givenDataToConnectMysqlToCheckDataBaseConnection") {
    val readDataFromMysql = lowestAverageHour
      .readDataFromMySqlForDataFrame("testDB", "test", username, password, url)
      .take(1)
    readDataFromMysql.foreach { row =>
      assert(row.get(0).toString === "2020-12-13 01:59:00.0")
      assert(row.get(1) === "vesha")
    }
  }
  test(
    "givenDataToConnectMysqlToCheckDataBaseConnectionAndShouldNotEqualToActual"
  ) {
    val readDataDF = lowestAverageHour
      .readDataFromMySqlForDataFrame("testDB", "test", username, password, url)
    val readDataFromMysql =
      lowestAverageHour.selectRequiredColumn(readDataDF).take(1)
    readDataFromMysql.foreach { row =>
      assert(row.get(0).toString != "2020-12-1:59:00.0")
      assert(row.get(1) != "wrong")
    }
  }
  test("DataFrameCountMustBeGreaterThanZero") {
    val userlogRowDF =
      lowestAverageHour.readDataFromMySqlForDataFrame(
        dbName,
        readTable,
        username,
        password,
        url
      )
    userlogRowDF.show()
    readRowData = lowestAverageHour.selectRequiredColumn(userlogRowDF).take(1)
    assert(userlogRowDF.count() > 0)
  }

  test("DateFrameTypeMustEqualToTheGivenType") {
    readRowData.foreach { row =>
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
    var time: Any = null
    var name: Any = null
    var keyboard: Any = null
    var mouse: Any = null
    readRowData.foreach { row =>
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
  test("checkTheDataOfDataFrameMustNotEqualToZero") {
    readRowData.foreach { row =>
      assert(row.get(0) != "")
      assert(row.get(1) != 0.2)
      assert(row.get(2) != 0.2)
      assert(row.get(3) != "")
    }
  }
  test(
    "givenDataFrameAsInputMustPerformSplitTimeAndDateShouldEqualToExceptedDataFrame"
  ) {
    minMaxTestDF = lowestAverageHour.splitAndCreateUserlogDF(splitTestDF)
    assert(minMaxTestDF.schema.toString() === splitSchemaDF)
    minMaxTestDF.take(1).foreach { row =>
      assert(row.get(0).toString === resultDFContent.head)
      assert(row.get(1).toString === resultDFContent(1))
      assert(row.get(2) === resultDFContent(2))
    }
  }
  test("givenMinMaxTestDFAsInputToFindMinAndMaxAndResultMustEqualToExpected") {
    dailyHourDF = lowestAverageHour.findMinAndMaxTimeForUsers(minMaxTestDF)
    assert(dailyHourDF.schema.toString() === minMaxSchema)
    dailyHourDF.take(1).foreach { row =>
      assert(row.get(0) === dailyHourData.head)
      assert(row.get(1).toString === dailyHourData(1))
      assert(row.get(2) === dailyHourData(2))
      assert(row.get(3) === dailyHourData.last)
    }
  }
  test("givenInputDFAsInputToCalculateDailyHourAndResultMustEqualToExpected") {
    hourTestDF = lowestAverageHour.calculateDailyHour(dailyHourDF)
    hourTestDF.take(1).foreach { rowData =>
      assert(rowData.get(0) === hourTestData.head)
      assert(rowData.get(1).toString === hourTestData(1))
      assert(rowData.get(2) === hourTestData.last)
    }
  }
  test("givenInputDFAsInputToSumDailyHoursAndResultMustEqualToExcepted") {
    sumHourDF = lowestAverageHour.sumDailyHourWRTUser(hourTestDF)
    val userAndHourMap = new mutable.HashMap[String, String]()
    sumHourDF.collect().foreach { user =>
      userAndHourMap.put(user.get(0).toString, user.get(1).toString)
    }
    assert(userAndHourMap("xyzname") == "9.0")
    assert(userAndHourMap("testname") == "0.0")
  }

  test("givenNullDataToReadDataFromMysqlItMustTriggerAnException") {
    val thrown = intercept[Exception] {
      lowestAverageHour.readDataFromMySqlForDataFrame(
        null,
        null,
        null,
        null,
        null
      )
    }
    assert(
      thrown.getMessage === "Parameters Are Null,Please Check The Passed Parameters"
    )
  }
  test("givenWrongDataToReadDataFromMysqlItMustTriggerAnException") {
    val thrown = intercept[Exception] {
      lowestAverageHour.readDataFromMySqlForDataFrame(
        dbName,
        readTable,
        username,
        password,
        ""
      )
    }
    assert(
      thrown.getMessage === "Parameters Are Null,Please Check The Passed Parameters"
    )
  }
  test(
    "givenInputDFForCreateBroadcastVariableCreationItMustCreateAndOutputMustEqualToExpected"
  ) {
    val testBVMap = lowestAverageHour.createIdleHoursBroadCast(sumHourDF)
    assert(testBVMap.value.getOrElse("xyzname", 0.0) === 9.0)
  }
  test(
    "givenInputDFForCreateBroadcastVariableCreationItMustCreateAndOutputMustNotEqualToExpected"
  ) {
    val testBVMap = lowestAverageHour.createIdleHoursBroadCast(sumHourDF)
    assert(testBVMap.value.getOrElse("xyzname", 0.0) != 0.0)
  }
  test(
    "givenDFAndBroadcastVariableItMustEvaluateAndOutputShouldMatchAsExpected"
  ) {
    val findHoursTestDF = lowestAverageHour.findHourByDifferencingIdleHours(
      sumHourDF,
      testBVIdleHour,
      1
    )
    findHoursTestDF
      .take(1)
      .foreach(row => {
        assert(row.getString(0) === "xyzname")
        assert(row.getDouble(1) === 7.0)
      })
  }
  test(
    "givenDFAndBroadcastVariableItMustEvaluateAndOutputShouldNotMatchAsExpected"
  ) {
    findHoursTestDF = lowestAverageHour.findHourByDifferencingIdleHours(
      sumHourDF,
      testBVIdleHour,
      1
    )
    findHoursTestDF
      .take(1)
      .foreach(row => {
        assert(row.getString(0) != "")
        assert(row.getDouble(1) != 0.0)
      })
  }
  test("givenDFItMustSortByLowToHighAndOutputMustEqualToAsExpected") {
    val highToLowDF =
      lowestAverageHour.sortByLowestAverageHours(findHoursTestDF)
    highToLowDF
      .take(1)
      .foreach(row => {
        assert(row.getString(0) === "testname")
        assert(row.getDouble(1) === 0.0)
      })
  }

  override def afterAll(): Unit = {
    spark.stop()
  }
}
