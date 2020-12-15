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
    try {
      dataFrame.createTempView("tableToWrite")
      val writeData = sparkSession.sql("""SELECT * FROM tableToWrite""")
      val prop = new Properties()
      prop.setProperty("user", System.getenv("MYSQL_UN"))
      prop.setProperty("password", System.getenv("MYSQL_PW"))
      writeData.write
        .mode("overwrite")
        .jdbc(System.getenv("URL") + dbName, tableName, prop)
      sparkSession.catalog.dropTempView("tableToWrite")
      true
    } catch {
      case nullPointerException: NullPointerException =>
        throw new Exception(
          "Null values received, Please Check the Passed Parameters"
        )
      case ex: Exception =>
        throw new Exception("Unable To Read Data From Mysql Database")
    }
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
    try {
      dataFrame
        .coalesce(1)
        .write
        .mode(SaveMode.Overwrite)
        .format("com.databricks.spark.xml")
        .option("rootTag", "Userlogs")
        .option("rowTag", "Userlog")
        .save(xmlFilePath)
      true
    } catch {
      case classNotFoundException: ClassNotFoundException =>
        throw new Exception(
          "com.databricks.spark.xml not found, Please specify the dependency"
        )
    }
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
