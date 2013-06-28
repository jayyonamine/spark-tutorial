package sparktutorial

import spark.SparkContext
import SparkContext._

object WordCount2 extends App {

  
  val sc = new SparkContext("spark://ec2-107-22-78-221.compute-1.amazonaws.com:7077", "Wordcount2", "/root/spark/", List("target/scala-2.9.2/spark-tutorial_2.9.2-0.1.jar"))
  
  val file = sc.textFile(hdfs://ec2-107-22-78-221.compute-1.amazonaws.com:9000/book.txt)
  val counts = file.flatMap(line => line.split(" "))
    .map(word => (word, 1))
    .reduceByKey(_ + _)
  counts.saveAsTextFile(/root/spark-tutorial/outputDir)
}
