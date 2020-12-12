package com.userloganalysisTest

import java.sql.Timestamp
import com.Utility.UtilityClass
import com.idelhours.IdleHours
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, to_timestamp}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import scala.collection.mutable

class IdleHoursTest extends FunSuite with BeforeAndAfterAll {
  var spark: SparkSession = _
  var idleHours: IdleHours = _
  val dbName: String = System.getenv("DB_NAME")
  val readTable: String = System.getenv("TABLE")
  val username: String = System.getenv("MYSQL_UN")
  val password: String = System.getenv("MYSQL_PW")
  val data = Seq(
    ("2019-05-21 06:05:02", "xyzname", 10.0, 41.00),
    ("2019-05-21 15:05:02", "testname", 0.0, 0.0),
    ("2019-05-21 07:05:02", "xyzname", 10.0, 0.0)
  )
  val column = Seq("datetime", "username", "keyboard", "mouse")
  var userlogReadTestDF: DataFrame = _
  var minMaxTestDF: DataFrame = _
  val keyMouseDataTest1 = Seq("xyzname", "2019-05-21 06:05:02", 1)
  val keyMouseDataTest2 = Seq("testname", "2019-05-21 15:05:02", 0)
  val grpConcatData = Seq(
    Seq("xyzname", "2019-05-21 WrappedArray(1, 1)"),
    Seq("testname", "2019-05-21 WrappedArray(0)")
  )
  val splitSchemaDF =
    "StructType(StructField(datetime,StringType,true), StructField(dates,DateType,true), StructField(username,StringType,true))"
  val minMaxSchema =
    "StructType(StructField(username,StringType,true), StructField(dates,DateType,true), StructField(login,StringType,true), StructField(logout,StringType,true))"
  val dailyHourData =
    Seq("xyzname", "2019-05-21", "2019-05-21 06:05:02", "2019-05-21 15:05:02")
  val hourTestData = Seq("xyzname", "2019-05-21", 9.0)
  var dailyHourDF: DataFrame = _
  var hourTestDF: DataFrame = _
  var keyMouseTestDF: DataFrame = _
  var readRowData: Array[Row] = _
  val zeroOrOne: Array[Int] =
    Array(1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0)
  val zeros = 13
  val idleTime = 1.08
  val testDFMap: mutable.LinkedHashMap[String, Double] =
    mutable.LinkedHashMap[String, Double]("xyzname" -> 10.0)
  override def beforeAll(): Unit = {
    spark = UtilityClass.createSparkSessionObj("Average lowest hour Test App")
    idleHours = new IdleHours(spark)
    val spark1 = spark
    import spark1.implicits._
    userlogReadTestDF = data.toDF(column: _*)
    userlogReadTestDF.withColumn("datetime", to_timestamp(col("datetime")))
  }
  test("givenDataToConnectMysqlToCheckDataBaseConnection") {
    val readDataFromMysql = idleHours
      .readDataFromMySqlForDataFrame("testDB", "test", username, password)
      .take(1)
    readDataFromMysql.foreach { row =>
      assert(row.get(0).toString === "2020-12-13 01:59:00.0")
      assert(row.get(1) === "vesha")
    }
  }
  test("DataFrameCountMustBeGreaterThanZero") {
    val userlogRowDF =
      idleHours.readDataFromMySqlForDataFrame(
        dbName,
        readTable,
        username,
        password
      )
    readRowData = userlogRowDF.take(1)
    assert(userlogRowDF.count() > 0)
  }

  test("DateFrameTypeMustToTheGivenType") {
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
  test("givenInputDataFrameMustPerformSomeConditionAndShouldReturnDF") {
    keyMouseTestDF =
      idleHours.analyzeKeyboardAndMouseDataFromTable(userlogReadTestDF)
    val keyMouseMap = new mutable.HashMap[String, String]()
    keyMouseTestDF
      .take(2)
      .foreach(data =>
        keyMouseMap.put(
          data.get(0).asInstanceOf[String],
          data.get(1).asInstanceOf[String] + " " + data
            .get(2)
        )
      )
    assert(
      keyMouseMap
        .getOrElse(keyMouseDataTest1.head.toString, 0) === keyMouseDataTest1(
        1
      ) + " " + keyMouseDataTest1(2)
    )
    assert(
      keyMouseMap
        .getOrElse(keyMouseDataTest2.head.toString, 0) === keyMouseDataTest2(
        1
      ) + " " + keyMouseDataTest2(2)
    )
  }
  test("givenDataFrameMustPerformGrpConcatAndDataFrameShouldEqualToExpected") {
    val guessIdleHrDF = idleHours.groupConcatTheData(keyMouseTestDF)
    val guessIdleHrMap = new mutable.HashMap[String, String]()
    guessIdleHrDF.collect().foreach { row =>
      guessIdleHrMap.put(
        row.get(0).asInstanceOf[String],
        "%s %s".format(
          row.get(1).asInstanceOf[String],
          row.get(2)
        )
      )
    }
    assert(
      guessIdleHrMap
        .getOrElse(grpConcatData.head.head, 0) === grpConcatData.head.last
    )
    assert(
      guessIdleHrMap
        .getOrElse(grpConcatData.last.head, 0) === grpConcatData.last.last
    )
  }
  test("givenArrayAsInputMustCheckNoOfZerosAndReturnDataMustEqualToActual") {
    val noOfZeros = idleHours.checkForZeros(zeroOrOne)
    assert(zeros === noOfZeros)
  }
  test("givenArrayAsInputMustCalculateTimeAndResultMustEqualToActual") {
    val time = idleHours.evaluateTime(zeroOrOne)
    assert(idleTime === time)
  }
  test("givenMapAsAnInputMustCreateDataFrame") {
    val dataFrameData = idleHours.createDataFrame(testDFMap).take(1)
    dataFrameData.foreach { row =>
      assert(row.get(0) === "xyzname")
      assert(row.get(1) === 10.0)
    }

  }
}
