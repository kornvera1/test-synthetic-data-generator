package com.synthetic.generators

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class AnomalyGenerator(spark: SparkSession) {
  import spark.implicits._

  private var currentDf: DataFrame = _

  def addAnomalies(df: DataFrame, field: String, anomalyFraction: Double): AnomalyGenerator = {
    currentDf = createAnomalies(df, field, anomalyFraction)
    this
  }

  def addAnomalies(field: String, anomalyFraction: Double): AnomalyGenerator = {
    require(currentDf != null, "You must call addAnomalies(df, field, fraction) first")
    currentDf = createAnomalies(currentDf, field, anomalyFraction)
    this
  }

  private def createAnomalies(df: DataFrame, field: String, anomalyFraction: Double): DataFrame = {
    val anomalyUdf = udf { (value: Double) =>
      if (scala.util.Random.nextDouble() < anomalyFraction) {
        field match {
          case "salary" => value * (if (scala.util.Random.nextBoolean()) 3 else 0.3)
          case "gpa" => if (value > 3.5) value * 0.7 else value * 1.5
          case _ => value * (if (scala.util.Random.nextBoolean()) 2 else 0.5)
        }
      } else {
        value
      }
    }

    df.withColumn(field, anomalyUdf(col(field)))
  }

  def build(): DataFrame = {
    require(currentDf != null, "No anomalies added")
    currentDf
  }
}