package com.synthetic.generators

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class NoiseGenerator(spark: SparkSession) {
  import spark.implicits._

  private var currentDf: DataFrame = _

  // Инициализирующий метод
  def addNoise(df: DataFrame, field: String, noiseLevel: Double): NoiseGenerator = {
    currentDf = applyNoise(df, field, noiseLevel)
    this
  }

  // Метод для добавления шума к текущему DF
  def addNoise(field: String, noiseLevel: Double): NoiseGenerator = {
    require(currentDf != null, "You must call addNoise(df, field, level) first")
    currentDf = applyNoise(currentDf, field, noiseLevel)
    this
  }

  def addCorrelatedNoise(sourceField: String, targetField: String, noiseLevel: Double): NoiseGenerator = {
    require(currentDf != null, "You must call addNoise first")
    val meanValue = currentDf.select(mean(sourceField)).first().getDouble(0)
    currentDf = currentDf.withColumn(targetField,
      col(targetField) + (col(sourceField) - meanValue) * noiseLevel * (rand() - 0.5))
    this
  }

  private def applyNoise(df: DataFrame, field: String, noiseLevel: Double): DataFrame = {
    // Правильная реализация добавления шума
    df.withColumn(field,
      col(field) * (lit(1.0) + (rand() - lit(0.5)) * lit(noiseLevel)))
  }

  def build(): DataFrame = {
    require(currentDf != null, "No noise added")
    currentDf
  }
}