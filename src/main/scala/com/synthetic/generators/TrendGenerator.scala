package com.synthetic.generators

import com.synthetic.models.TrendConfig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class TrendGenerator(spark: SparkSession) {
  import spark.implicits._

  private var currentDf: DataFrame = _

  def addTrends(df: DataFrame, field: String, config: TrendConfig): TrendGenerator = {
    currentDf = df.withColumn(field,
      when(col("graduation_year").isNotNull,
        lit(config.baseValue) +
          (lit(config.yearlyIncrease) * (lit(2023) - col("graduation_year"))))
            .otherwise(col(field))
      )
    this
  }

  def addTrends(field: String, config: TrendConfig): TrendGenerator = {
    require(currentDf != null, "You must call addTrends(df, field, config) first")
    currentDf = currentDf.withColumn(field,
      when(col("graduation_year").isNotNull,
        lit(config.baseValue) +
          (lit(config.yearlyIncrease) * (lit(2023) - col("graduation_year"))))
            .otherwise(col(field))
      )
    this
  }

  def addSeasonality(field: String, amplitude: Double): TrendGenerator = {
    require(currentDf != null, "You must call addTrends first")
    currentDf = currentDf.withColumn(field,
      col(field) * (lit(1.0) + lit(amplitude) *
        sin(lit(2 * math.Pi) * (lit(2023) - col("graduation_year")) / lit(10.0)))
    )
    this
  }

  def build(): DataFrame = {
    require(currentDf != null, "No transformations applied")
    currentDf
  }
}