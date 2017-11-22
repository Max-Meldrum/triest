import Utils.Edge
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

import scala.collection.mutable.ArrayBuffer
import scala.util.Random


object Triest extends App {
  val env = ExecutionEnvironment.getExecutionEnvironment

  // Well, in this case a file..
  val stream = env.readTextFile("data/out.moreno_blogs_blogs")

  val edges: DataSet[Edge[Int,Int]] = stream.filter(line => line.nonEmpty && !line.startsWith("%"))
      .map(_.split(" ") match {case Array(a, b) => (a.toInt, b.toInt)})

  val maxEdges = 100
  edges.map(new Triest[Int, Int](maxEdges))
    .print()

}


/** Following the implementation of the paper
  *
  * @param m Max Edges in our Sampling
  */
class Triest[A,B](m: Int) extends RichMapFunction[Edge[A,B], Double] {
  // Our counter
  private[this] var t = 0

  private[this] var sample = ArrayBuffer[Edge[A,B]]()


  // this will handle the logic for each "iteration"
  override def map(edge: Edge[A,B]): Double = {
    t += 1
    if (sampleEdge(edge, t)) {
      sample.append(edge)
      updateCounters(Increment, edge)
    }

    // return latest estimate..
    // Estimation in the paper
    // max = (1, (t(t-1)(t-2))/M(M-1)(M-2)))
    0.5
  }

  private def sampleEdge(edge: Edge[A,B], count: Int): Boolean = {
    if (count <= m) {
      true
    } else {
      flipBiasedCoin((m/count)) match {
        case Heads => {
          val randomEdge = sample(Random.nextInt(m))
          sample -= randomEdge
          sample += edge
          updateCounters(Decrement, randomEdge)
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

  private def updateCounters(op: Operation, edge: Edge[A,B]): Unit = {
    // Fetch neighbours..
    
    op match {
      case Increment =>
      case Decrement =>
    }
  }

}
