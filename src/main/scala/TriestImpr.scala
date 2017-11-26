import Utils.Edge
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/** Implementation of TRIÈST-IMPR, for undirected graphs.
  *
  * @param m Max Edges in our Sampling
  */
class TriestImpr[A](m: Int) extends RichMapFunction[Edge[A], Int] with TriestHelpers[A] {
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
    updateCounters(edge)
    if (sampleEdge(edge, t)) {
      sample.append(edge)
    }

    // Return an approximation of global triangles count
    tglobal
  }

  private def sampleEdge(edge: Edge[A], count: Int): Boolean = {
    if (count <= m) {
      true
    } else {
      flipBiasedCoin(m/count) match {
        case Heads => {
          val randomEdge = sample(Random.nextInt(m))
          sample -= randomEdge
          true
        }
        case Tails => false
      }
    }
  }

  private def updateCounters(edge: Edge[A]): Unit = {
    val shared = sharedNeighbors(sample, edge)
    val sharedSize = shared.size

    val weight = estimate(t.toLong, m.toLong)
      .toInt

    shared.foreach { item =>
      tlocal.put(edge._1, tlocal.getOrElse(edge._1, 0) + weight)
      tlocal.put(edge._2, tlocal.getOrElse(edge._2, 0) + weight)
      tlocal.put(item, tlocal.getOrElse(item, 0) + weight)
      tglobal += weight
    }
  }
}

object TriestImpr extends App {
  val env = ExecutionEnvironment.getExecutionEnvironment
  val maxEdges = 3000

  // Well, in this case a file..
  val stream = env.readTextFile("data/out.dolphins")
  val triestImpr = new TriestImpr[Int](maxEdges)

  val job = stream.filter(line => !line.startsWith("%"))
    .map(_.split("\\s+") match { case Array(a, b) => (a.toInt, b.toInt)})
    .map(triestImpr)
    .setParallelism(1)

  job.print()
}
