package com.synthetic.utils

import breeze.stats.distributions.Rand.VariableSeed.randBasis
import breeze.stats.distributions._
import org.apache.spark.sql.functions.udf

object RandomUtils {
  def normalDist(mean: Double, stdDev: Double) = udf(() => {
    new Gaussian(mean, stdDev).sample()
  }).asNondeterministic()

  def uniformDist(min: Double, max: Double) = udf(() => {
    new Uniform(min, max).sample()
  }).asNondeterministic()

  def logNormalDist(mean: Double, stdDev: Double) = udf(() => {
    new LogNormal(mean, stdDev).sample()
  }).asNondeterministic()

  def categoricalDist[T](items: Seq[(T, Double)]) = {
    val totalWeight = items.map(_._2).sum
    val normalized = items.map { case (value, weight) => (value, weight / totalWeight) }
    val cumulative = normalized.scanLeft((Option.empty[T], 0.0)) {
      case ((_, acc), (value, weight)) => (Some(value), acc + weight)
    }.tail

    udf(() => {
      val r = scala.util.Random.nextDouble()
      cumulative.find { case (_, cumWeight) => r <= cumWeight }
        .flatMap(_._1)
        .map(_.toString)
        .getOrElse("")
    }).asNondeterministic()
  }
}