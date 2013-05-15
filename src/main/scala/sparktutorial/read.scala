package 
import scala.math._
import scala.io.Source
import spark.util.Vector

object ReadFile extends Application {

  case class DataPoint(x: Vector, y: Double)
  var data: Array[DataPoint] = Array()

  var response: Double = 0
  var predictor: Vector = Vector()

  var whichline = 0
  for (line <- Source.fromFile("data.txt").getLines) {

    // parse the string
    val mystring = line.split(" ")
    response = mystring.head.toDouble
    predictor = mystring.tail.map(_.toDouble).toVector
    data = DataPoint(predictor,response) +: data

    whichline += 1
  }
  println("Read " + whichline + " lines of data")

}
