package sparktutorial

import spark.SparkContext
import SparkContext._
import spark._

object WordCount2 extends App {
 def main(args: Array[String]) {
  val sc = new SparkContext(args(1), "Wordcount2", "/root/spark/", List("target/scala-2.9.2/spark-tutorial_2.9.2-0.1.jar"))
  val file = sc.textFile(args(2)).cache()
  val counts = file.flatMap(line => line.split(" "))
    .map(word => (word, 1))
    .reduceByKey(_ + _)
  println(counts)
  }
}
