package com.userloganalysisTest

import com.Utility.UtilityClass
import com.Utility.WriteDataToSource.{
  writeDataFrameInToXMLFormat,
  writeDataFrameIntoJsonFormat,
  writeDataFrameToMysql
}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class WriteDataToSourceTest extends FunSuite with BeforeAndAfterAll {
  var sparkSession: SparkSession = _
  var dataFrame: DataFrame = _
  val dbName = "testDB"
  val tableName = "testTable"
  override def beforeAll(): Unit = {
    sparkSession = UtilityClass.createSparkSessionObj("Write Data to source")
    val spark = sparkSession
    import spark.implicits._
    dataFrame = Seq(("vesha", 1)).toDF("name", "id")
  }

  test("givenDataToWriteIntoAMysqlDBItMustWriteIntoTableAndMustReturnTrue") {
    val result = writeDataFrameToMysql(
      dataFrame,
      dbName,
      tableName
    )
    assert(result === true)
  }
  test("givenDataToWriteIntoAMysqlDBItMustThrowANullPointerException") {
    val thrown = intercept[Exception] {
      writeDataFrameToMysql(
        null,
        null,
        null
      )
    }
    assert(
      thrown.getMessage === "Null values received, Please Check the Passed Parameters"
    )
  }
  test("givenDataToWriteIntoAMysqlDBItMustThrowAnException") {
    val thrown = intercept[Exception] {
      writeDataFrameToMysql(
        dataFrame,
        "",
        tableName
      )
    }
    assert(
      thrown.getMessage === "Unable To Read Data From Mysql Database"
    )
  }

  test("givenDataToWriteIntoAJsonFileAndItMustReturnTrueAfterSuccessful") {
    val result = writeDataFrameIntoJsonFormat(dataFrame, "jsonTest")
    assert(result === true)
  }
  test("givenDataToWriteIntoAXMLFileAndItMustReturnTrueAfterSuccessful") {
    val result = writeDataFrameInToXMLFormat(dataFrame, "xmlTest")
    assert(result === true)
  }
  override def afterAll(): Unit = {
    sparkSession.stop()
  }
}
