package edu.ucr.dblab.kdtree

import KdTreeSingleThread.KdNode

import java.io.PrintWriter
import scala.io.Source
import scala.util.Random

object KdTree {
  def main(args: Array[String]): Unit = {
    val home = System.getenv("HOME")
    val buffer = Source.fromFile(s"$home/RIDIR/Datasets/sample5K.tsv")
    val coords = buffer.getLines().map{ _.split("\t").map(_.toInt) }.toArray
    buffer.close()

    val root = KdNode.createKdTree(coords)

    println("Sideways k-d tree with root on the left:\n")
    root.printKdTree(0)
    //makeSample(5000, "5K")
  }

  private def makeSample(n: Int, tag: String = "") : Unit = {
    val sample = for(_ <- 0 until n) yield {
      s"${Random.nextInt(n)}\t${Random.nextInt(n)}\n"
    }
    val filename = if(tag.isEmpty) s"/tmp/sample$n.tsv" else s"/tmp/sample$tag.tsv"
    val f = new PrintWriter(filename)
    f.write(sample.mkString(""))
    f.close()
  }
}