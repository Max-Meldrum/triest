import Utils.Edge

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

trait TriestHelpers[A] {

  def estimate(t: Long, m: Long): Double = {
    val estimate = (t * (t-1) * (t-2)) / (m * (m-1)*(m-2))
    Math.max(1, estimate).toDouble
  }

  def sharedNeighbors(sample: ArrayBuffer[Edge[A]], edge: Edge[A]): Set[A] = {
    val neighbors = (x: A) => sample.collect {
      case (u,v) if u == x => v
      case (u,v) if v == x => u
    }.toSet

    neighbors(edge._1) intersect neighbors(edge._2)
  }

  def flipBiasedCoin(value: Int): Coin = {
    Random.nextInt() < value match {
      case true => Heads
      case false => Tails
    }
  }
}
