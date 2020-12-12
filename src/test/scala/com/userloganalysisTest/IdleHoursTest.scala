package com.userloganalysisTest

import java.sql.Timestamp

import com.Utility.UtilityClass
import com.idelhours.IdleHours
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, to_timestamp}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.convert.ImplicitConversions.`seq AsJavaList`
import scala.collection.mutable

class IdleHoursTest extends FunSuite with BeforeAndAfterAll {
  var spark: SparkSession = _
  var idleHours: IdleHours = _
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

  override def beforeAll(): Unit = {
    spark = UtilityClass.createSparkSessionObj("Average lowest hour Test App")
    idleHours = new IdleHours(spark)
    val spark1 = spark
    import spark1.implicits._
    userlogReadTestDF = data.toDF(column: _*)
    userlogReadTestDF.withColumn("datetime", to_timestamp(col("datetime")))
  }

  test("DataFrameCountMustBeGreaterThanZero") {
    val userlogRowDF = idleHours.readDataFromMySqlForDataFrame()
    assert(userlogRowDF.count() > 0)
  }

  test("DateFrameTypeMustToTheGivenType") {
    val rowList = idleHours.readDataFromMySqlForDataFrame().take(1)
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
    val rowData = idleHours.readDataFromMySqlForDataFrame().take(1)
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
        .getOrElse(keyMouseDataTest1.head.toString, 0) === keyMouseDataTest1
        .get(1) + " " + keyMouseDataTest1.get(2)
    )
    assert(
      keyMouseMap
        .getOrElse(keyMouseDataTest2.head.toString, 0) === keyMouseDataTest2
        .get(1) + " " + keyMouseDataTest2.get(2)
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

}
