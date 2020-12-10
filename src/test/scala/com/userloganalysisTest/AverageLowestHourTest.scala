package com.userloganalysisTest

import java.sql.Timestamp
import com.userloganalysis.{AverageLowestHour, UtilityClass}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class AverageLowestHourTest extends FunSuite with BeforeAndAfterAll {
  var sparkSession: SparkSession = _
  var averageLowestHour: AverageLowestHour = _
  val csvFilePath = "./Data/CpuLogData2019-09-16.csv"
  val wrongCSV = "./Data/testCSV.csv"

  override def beforeAll(): Unit = {
    sparkSession =
      UtilityClass.createSparkSessionObj("Average lowest hour Test App")
    averageLowestHour = new AverageLowestHour

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
    rowData.foreach(a => println(a))
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

  override def afterAll(): Unit = {
    sparkSession.stop()
  }
}
