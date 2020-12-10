package com.userloganalysisTest

import org.apache.spark.sql.DataFrame

object DataFrameComparison {
  def compareDataFrame(actualDF: DataFrame, expectedDF: DataFrame): Boolean = {
    if (
      !actualDF.schema.toString().equalsIgnoreCase(expectedDF.schema.toString())
    ) {
      return false
    }
    if (
      actualDF
        .unionAll(expectedDF)
        .except(actualDF.intersect(expectedDF))
        .count() != 0
    ) {
      return false
    }
    true
  }
}
