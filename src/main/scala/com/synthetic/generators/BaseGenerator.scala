package com.synthetic.generators

import com.synthetic.config.DataConfig
import com.synthetic.models._
import com.synthetic.utils.RandomUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class BaseGenerator(spark: SparkSession, config: DataConfig) {
  import spark.implicits._

  def generate(): DataFrame = {
    val initialDf = spark.range(1, config.generationParams.numRecords + 1)
      .toDF("id")

    config.fields.foldLeft(initialDf) { case (df, (fieldName, distribution)) =>
      distribution match {
        case dist: NormalDistribution =>
          df.withColumn(fieldName, RandomUtils.normalDist(dist.mean, dist.stdDev)())
        case dist: UniformDistribution =>
          df.withColumn(fieldName, RandomUtils.uniformDist(dist.min, dist.max)())
        case dist: LogNormalDistribution =>
          df.withColumn(fieldName, RandomUtils.logNormalDist(dist.mean, dist.stdDev)())
        case dist: CategoricalDistribution[_] =>
          df.withColumn(fieldName, RandomUtils.categoricalDist(dist.items)())
        case _ => df
      }
    }
  }
}
