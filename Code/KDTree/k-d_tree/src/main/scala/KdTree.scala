package edu.ucr.dblab.kdtree

import KdTreeSingleThread.KdNode

import java.util
import scala.io.Source

object KdTree {
  def main(args: Array[String]): Unit = {
    val home = System.getenv("HOME")
    val buffer = Source.fromFile(s"$home/RIDIR/Datasets/dummy_kdtree.tsv")
    val coords = buffer.getLines().map{ _.split("\t").map(_.toInt) }.toArray
    buffer.close()


    val root = KdNode.createKdTree(coords)

    println("Sideways k-d tree with root on the left:\n")
    root.printKdTree(0)

    val maximumSearchDistance = 2
    val query = Array[Int](4, 3, 1)

    val kdNodes: util.List[KdTreeSingleThread.KdNode] = root.searchKdTree(query, maximumSearchDistance, 0)
    print("\n" + kdNodes.size + " nodes within " + maximumSearchDistance + " units of ")
    KdNode.printTuple(query)
    println(" in all dimensions.\n")
    if (!kdNodes.isEmpty) {
      println("List of k-d nodes within " + maximumSearchDistance + "-unit search distance follows:\n")
      for (i <- 0 until kdNodes.size) {
        val node: KdTreeSingleThread.KdNode = kdNodes.get(i)
        KdNode.printTuple(node.point)
        if (i < kdNodes.size - 1) print("  ")
      }
      println("\n")
    }
  }
}