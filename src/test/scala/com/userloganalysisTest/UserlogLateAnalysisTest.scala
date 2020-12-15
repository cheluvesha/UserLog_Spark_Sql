package com.userloganalysisTest

import java.sql.Timestamp

import com.Utility.UtilityClass
import com.highestNoOfTimeLateComing.UserlogLateAnalysis
import org.apache.spark.broadcast.Broadcast
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
  val data = Seq(
    Row("xyzname", "2019-10-12 09:00:00", "2019-10-12 10:30:00"),
    Row("testname", "2019-10-12 09:00:00", "2019-10-12 10:00:00"),
    Row("xyzname", "2019-10-13 09:00:00", "2019-10-13 10:30:00"),
    Row("testname", "2019-10-13 09:00:00", "2019-10-13 09:00:00")
  )
  val schema =
    List(
      StructField("user_name", StringType, nullable = true),
      StructField("entry_time", StringType, nullable = true),
      StructField("users_login_time", StringType, nullable = true)
    )

  var noOfLateComingDF: DataFrame = _
  var userlogLoginDF: DataFrame = _
  var lateComersDF: DataFrame = _
  var lateHoursDF: DataFrame = _
  var sumLateHourDF: DataFrame = _
  var noOfLateBroadCast: Broadcast[collection.Map[String, Long]] = _
  override def beforeAll(): Unit = {
    spark = UtilityClass.createSparkSessionObj("Late Analysis Test")
    userlogLateAnalysis = new UserlogLateAnalysis(spark)
    userlogLoginDF = spark.createDataFrame(
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
  test("givenDFItMustCountNoOfLateComingAndOutputShouldBeAsExpected") {
    noOfLateComingDF = userlogLateAnalysis.findLateComing(userlogLoginDF)
    noOfLateComingDF
      .take(1)
      .foreach(row => {
        assert(row.getString(0) === "xyzname")
        assert(row.getLong(1) === 2)
      })
  }
  test("givenDFItMustCountNoOfLateComingAndOutputShouldNotBeAsExpected") {
    noOfLateComingDF
      .take(1)
      .foreach(row => {
        assert(row.getString(0) != "xyzn")
        assert(row.getLong(1) != 1)
      })
  }
  test("givenWhenDFInputItShouldFindHighestNoOfLateComers") {
    lateComersDF =
      userlogLateAnalysis.findHighestNoOfTimeLateComers(noOfLateComingDF)
    lateComersDF
      .take(1)
      .foreach(row => {
        assert(row.getString(0) === "xyzname")
        assert(row.getLong(1) === 2)
      })
  }
  test(
    "givenWhenDFInputItShouldFindHighestNoOfLateComersAndOutputShouldNotEqualToExpected"
  ) {
    lateComersDF
      .take(1)
      .foreach(row => {
        assert(row.getString(0) != "")
        assert(row.getLong(1) != 1)
      })
  }
  test("givenDFAsInputToFindUsersLateHoursAndOutputMustEqualAsExpected") {
    lateHoursDF = userlogLateAnalysis.findUsersLateHours(userlogLoginDF)
    lateHoursDF
      .take(1)
      .foreach(row => {
        assert(row.getString(0) === "xyzname")
        assert(row.getDouble(1) === 1.5)
      })
  }
  test("givenDFAsInputToFindUsersLateHoursAndOutputMustNotEqualAsExpected") {
    lateHoursDF
      .take(1)
      .foreach(row => {
        assert(row.getString(0) != "xyze")
        assert(row.getDouble(1) != 0.0)
      })
  }
  test("givenDFAsInputToSumLateHoursOfUsersAndOutputMustEqualAsExpected") {
    sumLateHourDF = userlogLateAnalysis.sumLateHoursForEachUsers(lateHoursDF)
    sumLateHourDF
      .take(1)
      .foreach(row => {
        assert(row.getString(0) === "xyzname")
        assert(row.getDouble(1) === 3.0)
      })
  }
  test("givenDFAsInputToSumLateHoursOfUsersAndOutputMustNotEqualAsExpected") {
    sumLateHourDF
      .take(1)
      .foreach(row => {
        assert(row.getString(0) != "")
        assert(row.getDouble(1) != 0.0)
      })
  }
  test("givenDFAsInputToBroadCastAndOutputMustBeEqualToAsExpected") {
    noOfLateBroadCast =
      userlogLateAnalysis.broadcastNoOfTimesLateComing(noOfLateComingDF)
    assert(noOfLateBroadCast.value.getOrElse("xyzname", 0L) === 2)
  }
  test("givenDFAsInputToBroadCastAndOutputMustNotEqualToAsExpected") {

    assert(noOfLateBroadCast.value.getOrElse("testname", 0L) != 2)
  }
  test("givenDFAsInputToFindAverageLateHoursAndOutputMustBeEqualToExpected") {
    val averageLateHourDF =
      userlogLateAnalysis.findAverageLateHours(sumLateHourDF, noOfLateBroadCast)
    averageLateHourDF
      .take(1)
      .foreach(row => {
        assert(row.getString(0) === "xyzname")
        assert(row.getDouble(1) === 1.5)
      })
  }
  test("givenDFAsInputToFindAverageLateHoursAndOutputMustNotEqualToExpected") {
    val averageLateHourDF =
      userlogLateAnalysis.findAverageLateHours(sumLateHourDF, noOfLateBroadCast)
    averageLateHourDF
      .take(1)
      .foreach(row => {
        assert(row.getString(0) != null)
        assert(row.getDouble(1) != 0.0)
      })
  }
}
