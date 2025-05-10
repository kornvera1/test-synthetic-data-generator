package com.synthetic.models

case class TrendConfig(
                        baseValue: Double,
                        yearlyIncrease: Double,
                        seasonalAmplitude: Option[Double] = None,
                        noiseAmplitude: Option[Double] = None
                      ) {
  def calculateValue(year: Int, currentYear: Int = 2023): Double = {
    val base = baseValue + (currentYear - year) * yearlyIncrease
    val withSeason = seasonalAmplitude.map { amp =>
      base * (1 + amp * math.sin(2 * math.Pi * (currentYear - year) / 10))
    }.getOrElse(base)

    withSeason
  }
}
