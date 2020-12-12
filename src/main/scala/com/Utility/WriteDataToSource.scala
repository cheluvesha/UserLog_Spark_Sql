package com.Utility

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

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

  /***
    * Writes data into xml format
    * @param dataFrame DataFrame
    * @param xmlFilePath String
    * @return Boolean
    */
  def writeDataFrameInToXMLFormat(
      dataFrame: DataFrame,
      xmlFilePath: String
  ): Boolean = {
    dataFrame
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("com.databricks.spark.xml")
      .option("rootTag", "Userlogs")
      .option("rowTag", "Userlog")
      .save(xmlFilePath)
    true
  }

  /***
    * Writes data into Json format
    * @param dataFrame DataFrame
    * @param jsonFilePath String
    * @return Boolean
    */
  def writeDataFrameIntoJsonFormat(
      dataFrame: DataFrame,
      jsonFilePath: String
  ): Boolean = {
    dataFrame
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .json(jsonFilePath)
    true
  }
}
