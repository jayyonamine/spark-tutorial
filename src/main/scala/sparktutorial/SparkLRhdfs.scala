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
object SparkLRhdfs {
  var D = 4
  var ITERATIONS = 3
  val rand = new Random(42)
	
  	case class DataPoint(x: Vector, y: Double)
	
	def readPoint(s: String) = {
		val mystring = s.split("\t")
		val y = mystring.head.toDouble 
		val x = Vector(mystring.tail.map(_.toDouble))
		DataPoint(x,y)
	}
	


  def main(args: Array[String]) {
  println(args)
    if (args.length == 0) {
      System.err.println("Usage: SparkLR <master> [<slices>]")
      System.exit(1)
    }
    
    val sc = new SparkContext("local", "SparkLRhdfs", "/home/jayyonamine/devel/spark", List("target/scala-2.9.2/spark-tutorial_2.9.2-0.1.jar"))
    val data = sc.textFile("hdfs://ec2-54-226-253-122.compute-1.amazonaws.com:9000/data.tab").map(readPoints).cache()

    // Initialize w to a random value
    var w = Vector(D, _ => 2 * rand.nextDouble - 1)
    println("Initial w: " + w)

    for (i <- 1 to ITERATIONS) {
	    System.err.println("\n\n\n\nStarting iteration " + i + "\n\n\n\n")
      println("On iteration " + i)
      val gradient = data.map { p =>
        (1 / (1 + exp(-p.y * (w dot p.x))) - 1) * p.y * p.x
      }.reduce(_ + _)
      w -= gradient
    }

    println("Final w: " + w)
    System.exit(0)
  }
}
