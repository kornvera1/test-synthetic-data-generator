package com.synthetic.models

import breeze.stats.distributions.Rand.VariableSeed.randBasis
import breeze.stats.distributions._

sealed trait FieldDistribution {
  def sample(): Double
}

case class NormalDistribution(mean: Double, stdDev: Double) extends FieldDistribution {
  private val dist = new Gaussian(mean, stdDev)
  def sample(): Double = dist.sample()
}

case class UniformDistribution(min: Double, max: Double) extends FieldDistribution {
  private val dist = new Uniform(min, max)
  def sample(): Double = dist.sample()
}

case class LogNormalDistribution(mean: Double, stdDev: Double) extends FieldDistribution {
  private val dist = new LogNormal(mean, stdDev)
  def sample(): Double = dist.sample()
}

case class CategoricalDistribution[T](items: Seq[(T, Double)]) extends FieldDistribution {
  private val totalWeight = items.map(_._2).sum
  private val normalized = items.map { case (value, weight) => (value, weight / totalWeight) }
  private val cumulative = normalized.scanLeft((Option.empty[T], 0.0)) {
    case ((_, acc), (value, weight)) => (Some(value), acc + weight)
  }.tail

  def sample(): Double = {
    val r = scala.util.Random.nextDouble()
    cumulative.find { case (_, cumWeight) => r <= cumWeight }
      .flatMap(_._1)
      .map(_.toString.toDouble)
      .getOrElse(0.0)
  }

  def sampleValue[T](): T = {
    val r = scala.util.Random.nextDouble()
    cumulative.find { case (_, cumWeight) => r <= cumWeight }
      .flatMap(_._1.asInstanceOf[Option[T]])
      .getOrElse(items.head._1.asInstanceOf[T])
  }
}
