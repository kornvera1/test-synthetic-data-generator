package com.synthetic.config

case class GenerationParams(
                             numRecords: Int,
                             anomalyFraction: Double,
                             noiseLevel: Double,
                             baseSeed: Option[Long],
                             outputPath: String,
                             partitions: Int
                           )

object GenerationParams {
  val Default = GenerationParams(
    numRecords = 100000,
    anomalyFraction = 0.01,
    noiseLevel = 0.1,
    baseSeed = Some(42L),
    outputPath = "output/synthetic_data",
    partitions = 8
  )
}
