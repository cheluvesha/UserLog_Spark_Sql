package com.Utility

import java.util.Properties
import org.apache.spark.sql.{DataFrame, SparkSession}

/***
  * Class writes analyzed data to Mysql table, Json and Xml Format
  */
object WriteDataToSource {
  val sparkSession: SparkSession =
    UtilityClass.createSparkSessionObj("Write Data to source")

  /***
    * Writes DataFrame by creating View table into Mysql table
    * @param dataFrame DataFrame
    * @param dbName String
    * @param tableName String
    * @return Boolean
    */
  def writeDataFrameToMysql(
      dataFrame: DataFrame,
      dbName: String,
      tableName: String
  ): Boolean = {
    dataFrame.createTempView("tableToWrite")
    val writeData = sparkSession.sql("""SELECT * FROM tableToWrite""")
    val prop = new Properties()
    prop.setProperty("user", "user")
    prop.setProperty("password", "user00")
    writeData.write
      .mode("append")
      .jdbc("jdbc:mysql://localhost:3306/" + dbName, tableName, prop)
    true
  }
}
