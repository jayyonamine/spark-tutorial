package sparktutorial

import spark.SparkContext
import SparkContext._

import java.util.Random
import scala.math.exp
import spark.util.Vector
import spark._

import scala.math._
import scala.io.Source

// factory object
object Matrix {

  def apply(d: Double, n: Int): Matrix = {
    var newdata: Array[Double] = Array(d)
    for (i <- 1 until n) newdata = newdata ++ Array.fill(n)(0.0) ++ Array(d)
    new Matrix(newdata,n,n)
  }

}


class Matrix(d: Array[Double], r: Int, c: Int) {

  // check preconditions
  require(d.length == r*c)

  // fields
  val rows: Int = r
  val cols: Int = c
  val data: Array[Double] = d

  // print method
  override def toString = {
    var s: String = ""
    for (i <- 0 until rows; j <- 0 until cols) {
      val el: String = data(i*cols+j).toString
      val ws: String = if (i==0 && j==0) "" else if (j==0) "\n" else " "
      s = s + ws + el
    }
    s
  }

  // get/set functions
  def apply(x: Int, y: Int): Double = {
    require(x>0 & x<=rows & y>0 & y<=cols)
    data(cols*(x-1)+y-1)
  }

  def set(x: Int, y: Int, d: Double) {
    require(x>0 & x<=rows & y>0 & y<=cols)
    data(cols*(x-1)+y-1) = d
  }

  def row(x: Int) = {
    require(x>0 & x<=rows)
    new Range((x-1)*cols, x*cols, 1) map (x => data(x))
  }

  def col(x: Int) = {
    require(x>0 & x<=cols)
    new Range(x-1, rows*cols, cols) map (x => data(x))
  }

  def transpose: Matrix = {
    var newdata: Array[Double] = Array()
    for (j <- 1 to cols) newdata = newdata ++ col(j)
    new Matrix(newdata, cols, rows)
  }

  def LU: (Matrix,Matrix) = {
    require(rows==cols)
    val n = rows
    val a = Matrix(1,n)
    val b = Matrix(0,n)

    def flatten[A,B,C](t: ((A,B),C)) = (t._1._1, t._1._2, t._2)

    def f(i: Int, j: Int, m: Int) = {
      Range(0,n) zip a.row(i) zip b.col(j) map flatten filter (_._1 < m) map (x => x._2*x._3) reduce (_+_)
    }

    var tmp = 0.0
    for (j <- 1 to n; i <- 1 to n) {
      if (i<=j) {
        tmp = if (i==1) this(i,j) else this(i,j) - f(i,j,i-1)
        b.set(i,j,tmp)
      }
      else {
        tmp = if (j==1) this(i,j) else this(i,j) - f(i,j,j-1)
        tmp /= b(j,j)
        a.set(i,j,tmp)
      }
    }

    (a,b)
  }

  // operators
  def * (c: Double): Matrix = this.mult_const(c)
  def + (m: Matrix): Matrix = this.add(m)
  def - (m: Matrix): Matrix = this.add(m*(-1))
  def * (m: Matrix): Matrix = this.mult(m)
  def * (v: Vector[Double]): Vector[Double] = this.multv(v)

  // helper functions
  private def mult_const(c: Double): Matrix = {
    var newdata = this.data map (c*_)
    new Matrix(newdata, rows, cols)
  }

  private def add(m: Matrix): Matrix = {
    require(this.rows==m.rows && this.cols==m.cols)
    var newdata = this.data zip m.data map (x => x._1 + x._2)
    new Matrix(newdata, rows, cols)
  }

  private def mult(m: Matrix): Matrix = {
    require(this.cols==m.rows)
    val tmp1 = Range(0,rows) flatMap (x => Vector.fill(m.cols)(x+1))
    val tmp2 = Range(0,rows) flatMap (x => Range(1,m.cols+1))
    val newdata = tmp1 zip tmp2 map (x => vdp(this.row(x._1),m.col(x._2))) 
    new Matrix(newdata.toArray, this.rows, m.cols)
  }

  private def multv(v: Vector[Double]): Vector[Double] = {
    require(this.cols==v.length)
    val r = Range(0,rows) map (x => vdp(row(x+1),v)) 
    r.toVector
  }

  private def vdp(v1: IndexedSeq[Double], v2: IndexedSeq[Double]): Double = {
    v1 zip v2 map (x => x._1 * x._2) reduceLeft (_+_)
  }

}



object Regression extends App {

val sc = new SparkContext("local", "ry", "/home/jayyonamine/devel/spark", List("target/scala-2.9.2/spark-tutorial_2.9.2-0.1.jar"))
    
  // square matrix with same dimension as b
  def LU_Solve(A: Matrix, b: Vector[Double]): Vector[Double] = {

    val n = b.length
    require(A.rows==n && A.cols==n)
    val ALU = A.LU

    val A2: Matrix = ALU._1
    var y: Array[Double] = Array.fill(n)(0)
    y(0) = b(0)
    def f(i: Int) = {
      A2.row(i) zip y map (x => x._1*x._2) reduce (_+_)
    }
    for (i <- 2 to n) {
      y(i-1) = b(i-1) - f(i)
    }

    val A3: Matrix = ALU._2
    var x: Array[Double] = Array.fill(n)(0)
    x(n-1) = y(n-1)/A3(n,n)
    def g(i: Int) = {
      A3.row(i) zip x map (x => x._1*x._2) reduce (_+_)
    }
    for (i <- n-1 to 1 by -1) {
      x(i-1) = ( y(i-1) - g(i) )/A3(i,i)
    }

    x.toVector
  }

  val nlines = Source.fromFile("s3://rbisd.spark/data.txt").getLines.length
  var response: Array[Double] = Array.fill(nlines)(0.0)
  var data: Array[Double] = Array.fill(6*nlines)(0.0)
  var whichline = 0
  var whichdataelement = 0
  for (line <- Source.fromFile("s3://rbisd.spark/data.txt").getLines) {

    // parse the string
    val mystring = line.split(" ")
    response(whichline) = mystring(0).toDouble

    // column of 1s for the intercept
    data(whichdataelement) = 1
    whichdataelement += 1
    // read in the remaining columns
    for (i <- 1 to 5) {
      data(whichdataelement) = mystring(i).toDouble
      whichdataelement += 1
    }
    whichline += 1
  }
  println("Read " + nlines + " lines of data")

  val columns = data.length/nlines
  val dataM = new Matrix(data,nlines,columns)

  val dataMT = dataM.transpose
  val A: Matrix = dataMT * dataM
  val b: Vector[Double] = dataMT * response.toVector
  val parameters: Vector[Double] = LU_Solve(A,b)
  println("\nParameters:\n"+parameters.map(x => x.toString + "\n").reduce(_+_))

}


Regression.main(args)




