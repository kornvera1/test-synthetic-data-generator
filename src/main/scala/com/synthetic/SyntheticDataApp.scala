package com.synthetic

import org.apache.spark.sql.SparkSession
import com.synthetic.config._
import com.synthetic.generators._
import com.synthetic.utils._
import com.synthetic.models._

object SyntheticDataApp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SyntheticDataGenerator")
      .master("local[*]")
      .getOrCreate()

    try {
      val params = GenerationParams.Default.copy(
        numRecords = args.headOption.map(_.toInt).getOrElse(100000)
      )

      val dataConfig = DataConfig(
        fields = Map(
          "age" -> UniformDistribution(18, 38),
          "gpa" -> NormalDistribution(3.0, 0.5),
          "salary" -> LogNormalDistribution(10.5, 0.5),
          "major" -> CategoricalDistribution(Seq(
            ("Computer Science", 0.25),
            ("Mathematics", 0.1),
            ("Physics", 0.08)
          )),
          "university" -> CategoricalDistribution(Seq(
            ("Harvard", 0.15),
            ("Stanford", 0.15),
            ("MIT", 0.15)
          )),
          "graduation_year" -> UniformDistribution(2013, 2023)
        ),
        relationships = List(
          FieldRelationship("gpa", "salary", 0.6, Linear),
          FieldRelationship("university", "salary", 0.4, Linear)
        ),
        generationParams = params
      )

      val baseDf = new BaseGenerator(spark, dataConfig).generate()

      val trendDf = new TrendGenerator(spark)
        .addTrends(baseDf, "salary", TrendConfig(50000, 1500, Some(0.05)))
        .addTrends("gpa", TrendConfig(3.0, 0.02))
        .build()

      val noisyDf = new NoiseGenerator(spark)
        .addNoise(trendDf, "salary", params.noiseLevel)
        .addNoise("gpa", params.noiseLevel * 0.5)
        .build()

      val anomalyDf = new AnomalyGenerator(spark)
        .addAnomalies(noisyDf, "salary", params.anomalyFraction)
        .addAnomalies("gpa", params.anomalyFraction)
        .build()

      DataFrameUtils.analyzeNumericFields(anomalyDf, Seq("age", "gpa", "salary"))
      DataFrameUtils.saveAsParquet(anomalyDf, params.outputPath, params.partitions)

    } finally {
      spark.stop()
    }
  }
}
