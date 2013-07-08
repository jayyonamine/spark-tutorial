package sparktutorial
import spark.SparkContext
import SparkContext._

import java.util.Random
import scala.math.exp
import spark.util.Vector
import spark._

/**
 * Logistic regression based classification.
 */
object SparkLRhdfs2 {

  val rand = new Random(42)

  	case class DataPoint(x: Vector, y: Double)

	def readPoint(s: String) = {
		val mystring = s.split(",")
		val y = mystring.head.toDouble 
		val x = Vector(mystring.tail.map(_.toDouble))
		DataPoint(x,y)
	}

  def main(args: Array[String]) {
  println(args.reduce(_+" "+_))
    if (args.length == 0) {
      System.err.println("Usage: SparkLR <master> [<slices>]")
      System.exit(1)
    }
    var D = args(1).toInt
    var ITERATIONS = args(2).toInt
    val sc = new SparkContext(args(5), "SparkLRhdfs2", "/root/spark/", List("target/scala-2.9.3/sparktutorial_2.9.3-0.1.jar"))
    val data = sc.textFile(args(3)).map(readPoint).cache()

    // Initialize w to a random value
    var w = Vector(D, _ => 0)
    println("Initial w: " + w)

    for (i <- 1 to ITERATIONS) {
	    System.err.println("\n\n\n\nStarting iteration " + i + "\n\n\n\n")
      println("On iteration " + i)
      val gradient = data.map { p =>
        (1 / (1 + exp(-p.y * (w dot p.x)))-1) * p.y * p.x
      }.reduce(_ + _)
      w -= (1.0 / (args(4).toDouble))*gradient  
    }

    println("Final w: " + w)
    System.exit(0)
  }
}
