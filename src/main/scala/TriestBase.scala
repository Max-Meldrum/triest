import Utils.Edge
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/** Implementation of TRIÈST-BASE, for undirected graphs.
  *
  * @param m Max Edges in our Sampling
  */
class TriestBase[A](m: Int) extends RichMapFunction[Edge[A], Int] with TriestHelpers[A] {
  // Counter of current items processed
  private[this] var t = 0
  // Counter of current global triangles
  private[this] var tglobal = 0
  // HashMap of local triangles
  private[this] var tlocal = scala.collection.mutable.HashMap[A,Int]()
  // Our container, that maximum holds m edges
  private[this] var sample = ArrayBuffer[Edge[A]]()

  override def map(edge: Edge[A]): Int = {
    t += 1
    if (sampleEdge(edge, t)) {
      sample.append(edge)
      updateCounters(Increment, edge)
    }

    // Return an approximation of global triangles count
    (estimate(t.toLong, m.toLong) * tglobal).toInt
  }

  private def sampleEdge(edge: Edge[A], count: Int): Boolean = {
    if (count <= m) {
      true
    } else {
      flipBiasedCoin(m/count) match {
        case Heads => {
          val randomEdge = sample(Random.nextInt(m))
          sample -= randomEdge
          updateCounters(Decrement, randomEdge)
          true
        }
        case Tails => false
      }
    }
  }


  private def updateCounters(op: Operation, edge: Edge[A]): Unit = {
    val shared = sharedNeighbors(sample, edge)
    val sharedSize = shared.size

    op match {
      case Increment => {
        tglobal += sharedSize
        tlocal.put(edge._1, tlocal.getOrElse(edge._1, 0) + sharedSize)
        tlocal.put(edge._2, tlocal.getOrElse(edge._2, 0) + sharedSize)
        shared.foreach {item => tlocal.put(item, tlocal.getOrElse(item, 0) + 1)}
      }
      case Decrement => {
        tglobal -= sharedSize
        val e1 = tlocal.getOrElse(edge._1, 0)
        val e2 = tlocal.getOrElse(edge._2, 0)

        if (e1 <= 0)
          tlocal.remove(edge._1)
        else
          tlocal.put(edge._1, e1 - sharedSize)


        if (e2 <= 0)
          tlocal.remove(edge._2)
        else
          tlocal.put(edge._2, e2 - sharedSize)

        shared.foreach {item => tlocal.put(item, tlocal.getOrElse(item, 0) - 1)}
      }
    }
  }
}

object TriestBase extends App {
  val env = ExecutionEnvironment.getExecutionEnvironment
  val maxEdges = 3000

  // Well, in this case a file..
  val stream = env.readTextFile("data/out.dolphins")
  val triestBase = new TriestBase[Int](maxEdges)

  val job = stream.filter(line => !line.startsWith("%"))
    .map(_.split("\\s+") match { case Array(a, b) => (a.toInt, b.toInt)})
    .map(triestBase)
    .setParallelism(1)

  job.print()
}
