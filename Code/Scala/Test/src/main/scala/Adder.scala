package edu.ucr.dblab

import scala.io.Source
import java.io.FileWriter

object Adder {

  def main(args: Array[String]): Unit = {
    val filename = "/home/acald013/Datasets/CA/A2.wkt"
    val offset = 5000
    val letter = "A"

    val data = Source.fromFile(filename).getLines.toList
    val n = data.size
    val ids = offset + 1 to offset + n
    val new_data = data.zip(ids).map{ row =>
      val line = row._1.split("\t")(0)
      val nid = row._2
      s"$line\t$letter$nid\n"
    }.mkString("")
    println(s"Size ${new_data.toList.size}")
    val f = new FileWriter("/tmp/A_prime.wkt")
    f.write(new_data)
    f.close
  }
}
