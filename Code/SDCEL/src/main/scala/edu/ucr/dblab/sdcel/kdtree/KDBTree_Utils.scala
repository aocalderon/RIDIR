package edu.ucr.dblab.sdcel.kdtree

import com.vividsolutions.jts.geom.{Envelope, GeometryFactory, Polygon}
import edu.ucr.dblab.sdcel.kdtree.KDBTree.Visitor

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable

object KDBTree_Utils {

  case class Kdbnode(lineage: String, envelope: Envelope, splitX: Boolean){
    def wkt(implicit G: GeometryFactory): String = {
      val wkt = G.toGeometry(envelope)
      val spx = if(splitX) 1 else 0
      s"$wkt\t${lineage}_$spx"
    }
  }
  def getItems(tree: KDBTree): mutable.HashMap[Int, List[Envelope]] = {
    val matches: mutable.HashMap[Int, List[Envelope]] = new mutable.HashMap[Int, List[Envelope]]()
    tree.traverse( new Visitor {
      override def visit(tree: KDBTree): Boolean = {
        if (tree.isLeaf) matches.put(tree.getLeafId, tree.getItems.asScala.toList)
        true
      }
    })

    matches
  }

  def getLineages(tree: KDBTree): mutable.HashMap[Int, String] = {
    val matches: mutable.HashMap[Int, String] = new mutable.HashMap[Int, String]()
    tree.traverse( new Visitor {
      override def visit(tree: KDBTree): Boolean = {
        if (tree.isLeaf) matches.put(tree.getLeafId, tree.lineage)
        true
      }
    })

    matches
  }
  def getCells(tree: KDBTree)(implicit G: GeometryFactory): mutable.HashMap[Integer, Polygon] = {
    val matches: mutable.HashMap[Integer, Polygon] = new mutable.HashMap[Integer, Polygon]
    tree.traverse(new KDBTree.Visitor() {
      override def visit(tree: KDBTree): Boolean = {
        if (tree.isLeaf) {
          val id: Int = tree.getLeafId
          val mbr: Polygon = G.toGeometry(tree.getExtent).asInstanceOf[Polygon]
          mbr.setUserData(id + "\t" + tree.lineage + "\t" + tree.splitX)
          matches.put(id, mbr)
        }
        true
      }
    })
    matches
  }

  def getAllCells(tree: KDBTree)(implicit G: GeometryFactory): mutable.HashMap[String, Kdbnode] = {
    val matches: mutable.HashMap[String, Kdbnode] = new mutable.HashMap[String, Kdbnode]()
    tree.traverse(new KDBTree.Visitor() {
      override def visit(tree: KDBTree): Boolean = {
        val envelope = tree.getExtent
        val lineage  = tree.lineage
        val splitX   = tree.splitX == 1
        val node = Kdbnode(lineage, envelope, splitX)
        matches.put(lineage, node)
        true
      }
    })
    matches
  }

}
