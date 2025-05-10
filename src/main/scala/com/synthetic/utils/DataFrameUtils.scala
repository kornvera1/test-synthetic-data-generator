package com.synthetic.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object DataFrameUtils {
  def analyzeNumericFields(df: DataFrame, fields: Seq[String]): Unit = {
    fields.foreach { field =>
      df.select(
        count(col(field)).as("count"),
        mean(col(field)).as("mean"),
        stddev(col(field)).as("stddev"),
        min(col(field)).as("min"),
        max(col(field)).as("max"),
        approx_count_distinct(col(field)).as("distinct")
      ).show(truncate = false)
    }
  }

  def saveAsParquet(df: DataFrame, path: String, partitions: Int = 8): Unit = {
    df.coalesce(partitions)
      .write
      .mode("overwrite")
      .parquet(path)
  }
}
