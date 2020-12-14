package com.userloganalysisTest

import java.sql.Timestamp

import com.Utility.UtilityClass
import com.highestNoOfTimeLateComing.UserlogLateAnalysis
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class UserlogLateAnalysisTest extends FunSuite with BeforeAndAfterAll {
  var spark: SparkSession = _
  var userlogLateAnalysis: UserlogLateAnalysis = _
  val hdfsFilePath =
    "hdfs://localhost:54310//userlogs_header/CpuLogData2019-09-16.csv"
  override def beforeAll(): Unit = {
    spark = UtilityClass.createSparkSessionObj("Late Analysis Test")
    userlogLateAnalysis = new UserlogLateAnalysis(spark)
  }

  test("givenHdfsFilePathItMustReadAndCreateDFAndCountMustGreaterThanZero") {
    val userlogDF = userlogLateAnalysis.readFilesFromHDFS(hdfsFilePath)
    assert(userlogDF.count() > 0)
  }
  test("givenHdfsFilePathItMustReadAndCreateDFSchemaMustMatchAsExpected") {
    val userlogDF = userlogLateAnalysis.readFilesFromHDFS(hdfsFilePath)
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

}
