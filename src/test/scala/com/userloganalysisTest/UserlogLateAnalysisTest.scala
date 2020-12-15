package com.userloganalysisTest

import java.sql.Timestamp

import com.Utility.UtilityClass
import com.highestNoOfTimeLateComing.UserlogLateAnalysis
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class UserlogLateAnalysisTest extends FunSuite with BeforeAndAfterAll {
  var spark: SparkSession = _
  var userlogLateAnalysis: UserlogLateAnalysis = _
  val hdfsFilePath =
    "hdfs://localhost:54310//userlogs_header/CpuLogData2019-09-16.csv"
  val wrongHDFSPath = "hdfs://localhost:54310//urlogs_header"
  val dailyHourData =
    Seq("rahilstar11@gmail.com", "2019-09-16", "2019-09-16 12:55:03.0")
  var userlogDF: DataFrame = _
  var selectedDF: DataFrame = _
  var findLoginTimeDF: DataFrame = _
  val findLoginTimeData =
    Seq("iamnzm@outlook.com", "2019-09-16", "2019-09-16 13:00:01.0")
  var appendedTimeDF: DataFrame = _
  val data = Seq(Row("xyzname", "2019-10-12", "2019-10-12 10:30:00"))
  val schema =
    List(
      StructField("user_name", StringType, nullable = true),
      StructField("dates", StringType, nullable = true),
      StructField("users_login_time", StringType, nullable = true)
    )
  var dfForAppendTest: DataFrame = _
  override def beforeAll(): Unit = {
    spark = UtilityClass.createSparkSessionObj("Late Analysis Test")
    userlogLateAnalysis = new UserlogLateAnalysis(spark)
    dfForAppendTest = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      StructType(schema)
    )
  }

  test("givenHdfsFilePathItMustReadAndCreateDFAndCountMustGreaterThanZero") {
    userlogDF = userlogLateAnalysis.readFilesFromHDFS(hdfsFilePath)
    assert(userlogDF.count() > 0)
  }
  test("givenHdfsFilePathItMustReadAndCreateDFSchemaMustMatchAsExpected") {
    userlogDF
      .take(1)
      .foreach(row => {
        assert(
          row.get(0).isInstanceOf[Timestamp],
          "zero column should be Timestamp"
        )
        assert(
          row.get(40).isInstanceOf[String],
          "Forty column should be String"
        )
      })
  }
  test("givenWrongHDFSFilePathMustTriggerAnException") {
    val thrown = intercept[Exception] {
      userlogLateAnalysis.readFilesFromHDFS(wrongHDFSPath)
    }
    assert(thrown.getMessage === "Please Check The Path Specified")
  }

  test("givenDFItMustSelectRequiredColumnsAndOutputShouldBeEqualToExpected") {
    selectedDF = userlogLateAnalysis.selectRequiredColumn(userlogDF)
    selectedDF
      .take(1)
      .foreach(row => {
        assert(row.get(0).toString === dailyHourData.last)
        assert(row.get(1).toString === dailyHourData(1))
        assert(row.get(2) === dailyHourData.head)
      })
  }
  test(
    "givenDFItMustSelectRequiredColumnsAndOutputShouldNotBeEqualToExpected"
  ) {
    selectedDF
      .take(1)
      .foreach(row => {
        assert(row.get(0) != "")
        assert(row.get(1) != "")
        assert(row.get(2) != null)
      })
  }
  test("givenDFAsInputItMustFindLoginTimeAndOutputShouldBeEqualAsExpected") {
    findLoginTimeDF = userlogLateAnalysis.findLoginTimeForUsers(selectedDF)
    findLoginTimeDF
      .take(1)
      .foreach(row => {
        assert(row.get(0) === findLoginTimeData.head)
        assert(row.get(1).toString === findLoginTimeData(1))
        assert(row.get(2).toString === findLoginTimeData(2))
      })
  }
  test("givenDFAsInputItMustFindLoginTimeAndOutputShouldNotBeEqualAsExpected") {
    findLoginTimeDF
      .take(1)
      .foreach(row => {
        assert(row.get(0) != null)
        assert(row.get(1) != null)
        assert(row.get(2) != null)
      })
  }
  test(
    "givenDateStringItMustAppendItToDateColAndOutputShouldBeEqualAsExpected"
  ) {
    appendedTimeDF = userlogLateAnalysis.appendActualLoginTimeToDate(
      dfForAppendTest,
      "09-00-00"
    )
    appendedTimeDF
      .take(1)
      .foreach(row => {
        assert(row.get(0) === "xyzname")
        assert(row.get(1) === "2019-10-12 09:00:00")
        assert(row.get(2) === "2019-10-12 10:30:00")
      })
  }
  test(
    "givenDateStringItMustAppendItToDateColAndOutputShouldNotBeEqualAsExpected"
  ) {
    appendedTimeDF = userlogLateAnalysis.appendActualLoginTimeToDate(
      dfForAppendTest,
      "09-00-00"
    )
    appendedTimeDF
      .take(1)
      .foreach(row => {
        assert(row.get(0) != null)
        assert(row.get(1) != null)
        assert(row.get(2) != null)
      })
  }

}
