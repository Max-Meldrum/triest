import Triest.Edge
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

sealed trait Coin
case object Heads extends Coin
case object Tails extends Coin

object Triest extends App {
  type Edge[A,B] = (A,B)
  val env = ExecutionEnvironment.getExecutionEnvironment

  val stream = env.readTextFile("data/out.moreno_blogs_blogs")
}


/** Following the implementation of the paper
  *
  * @param m Max Edges
  */
class Triest[A,B](m: Int) extends RichMapFunction[Edge[A,B], Int] {
  // Our counter
  private var t = 0

  // S in the
  private var sample = ArrayBuffer[Edge[A,B]]()


  // this will handle the logic for each "iteration"
  override def map(edge: Edge[A,B]) = {
    t += 1
    if (sampleEdge(edge, t)) {
      sample.append(edge)
      // update counter
    }
    1
  }

  private def sampleEdge(edge: Edge[A,B], count: Int): Boolean = {
    if (count <= m) {
      true
    } else {
      flipBiasedCoin((m/count)) match {
        case Heads => {

          true
        }
        case Tails => false
      }
    }
  }

  private def flipBiasedCoin(value: Double): Coin = {
    Random.nextDouble() < value match {
      case true => Heads
      case false => Tails
    }
  }
}
