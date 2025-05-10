package com.synthetic

import org.apache.spark.sql.SparkSession
import com.synthetic.config._
import com.synthetic.generators._
import com.synthetic.utils._
import com.synthetic.models._

object SyntheticDataApp {
  def main(args: Array[String]): Unit = {
    // Инициализация Spark
    val spark = SparkSession.builder()
      .appName("SyntheticDataGenerator")
      .master("local[*]")
      .getOrCreate()

    try {
      // Конфигурация генерации
      val params = GenerationParams.Default.copy(
        numRecords = args.headOption.map(_.toInt).getOrElse(100000)
      )

      // Определение схемы данных
      val dataConfig = DataConfig(
        fields = Map(
          "age" -> UniformDistribution(18, 38),
          "gpa" -> NormalDistribution(3.0, 0.5),
          "salary" -> LogNormalDistribution(10.5, 0.5),
          "major" -> CategoricalDistribution(Seq(
            ("Computer Science", 0.25),
            ("Mathematics", 0.1),
            ("Physics", 0.08),
            // ... другие специальности
          )),
          "university" -> CategoricalDistribution(Seq(
            ("Harvard", 0.15),
            ("Stanford", 0.15),
            ("MIT", 0.15),
            // ... другие университеты
          )),
          "graduation_year" -> UniformDistribution(2013, 2023)
        ),
        relationships = List(
          FieldRelationship("gpa", "salary", 0.6, Linear),
          FieldRelationship("university", "salary", 0.4, Linear)
        ),
        generationParams = params
      )

      // Генерация данных
      val baseDf = new BaseGenerator(spark, dataConfig).generate()

      // Добавление трендов (с вызовом build())
      val trendDf = new TrendGenerator(spark)
        .addTrends(baseDf, "salary", TrendConfig(50000, 1500, Some(0.05)))
        .addTrends("gpa", TrendConfig(3.0, 0.02))
        .build() // <-- Добавлен build()

      // Добавление шума (с вызовом build())
      val noisyDf = new NoiseGenerator(spark)
        .addNoise(trendDf, "salary", params.noiseLevel)
        .addNoise("gpa", params.noiseLevel * 0.5)
        .build() // <-- Добавлен build()

      // Добавление аномалий (с вызовом build())
      val anomalyDf = new AnomalyGenerator(spark)
        .addAnomalies(noisyDf, "salary", params.anomalyFraction)
        .addAnomalies("gpa", params.anomalyFraction)
        .build() // <-- Добавлен build()

      // Анализ и сохранение
      DataFrameUtils.analyzeNumericFields(anomalyDf, Seq("age", "gpa", "salary"))
      DataFrameUtils.saveAsParquet(anomalyDf, params.outputPath, params.partitions)

    } finally {
      spark.stop()
    }
  }
}
