package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.geom.{Polygon, LineString, Coordinate}
import com.vividsolutions.jts.io.WKTReader
import com.vividsolutions.jts.algorithm.CGAlgorithms

import edu.ucr.dblab.sdcel.geometries.{Half_edge, Vertex, EdgeData, HEdge,Coords}
import edu.ucr.dblab.sdcel.DCELMerger2.merge2
import edu.ucr.dblab.sdcel.DCELOverlay2.{overlay3, overlay2}
import scala.io.Source
import scala.annotation.tailrec

object Test3 {
  def main(args: Array[String]): Unit = {
    val model = new PrecisionModel(1000)
    implicit val geofactory = new GeometryFactory(model)
    implicit val reader = new WKTReader(geofactory)

    println("In Test3")
    val c1 = new Coordinate(2,6)
    val c2 = new Coordinate(1,7)
    val c3 = new Coordinate(0,5)
    val c4 = new Coordinate(2,5)
    val a1 = Array(c1,c2,c3,c4)

    val c5 = new Coordinate(2,5)
    val c6 = new Coordinate(4,3)
    val c7 = new Coordinate(4,5)
    val c8 = new Coordinate(2,6)
    val a2 = Array(c5,c6,c7,c8)

    val c9 = new Coordinate(9,1)
    val c10 = new Coordinate(8,2)
    val c11 = new Coordinate(5,2)
    val c12  = new Coordinate(6,1)
    val a3 = Array(c9,c10,c11,c12)

    val c13 = new Coordinate(6,1)
    val c14 = new Coordinate(7,0)
    val c15 = new Coordinate(10,0)
    val c16 = new Coordinate(9,1)
    val a4 = Array(c13,c14,c15,c16)

  }

  def createHEdge(c1: Coordinate, c2: Coordinate)
      (implicit geofactory: GeometryFactory): Half_edge = {
    val e = geofactory.createLineString(Array(c1,c2))
    val ed = EdgeData(1,0,1,false)
    e.setUserData(ed)
    val h = Half_edge(e)
    h
  }

  def readWKT(filename: String, label: String)
    (implicit reader: WKTReader, geofactory: GeometryFactory): List[Half_edge] = {

    val buffer = Source.fromFile(filename)
    val h = buffer.getLines.zipWithIndex.flatMap{ case(line, polygonId) =>
      val arr = line.split("\t")
      val polygon = reader.read(arr(0)).asInstanceOf[Polygon]

      val ring = polygon.getExteriorRing.getCoordinates

      val coords = if(!CGAlgorithms.isCCW(ring)) {
        ring.reverse
      } else {
        ring
      }

      coords.zip(coords.tail).zipWithIndex.map{ case(coords, edgeId) =>
        val arr = Array(coords._1, coords._2)
        val edge = geofactory.createLineString(arr)
        val data = EdgeData(polygonId, 0, edgeId, false, label)
        edge.setUserData(data)
        Half_edge(edge)
      }
    }.toList.filter(h => h.v1 != h.v2)
    val h_prime = h :+ h.head
    h_prime.zip(h_prime.tail).foreach{ case(h1, h2) =>
      h1.next = h2
      h2.prev = h1
    }
    buffer.close

    h
  }

  def readWKT2(filename: String, label: String, id: Int)
    (implicit reader: WKTReader, geofactory: GeometryFactory): List[Half_edge] = {

    val buffer = Source.fromFile(filename)
    val h = buffer.getLines.zipWithIndex.flatMap{ case(line, polygonId) =>
      val arr = line.split("\t")
      val polygon = reader.read(arr(0)).asInstanceOf[Polygon]

      val ring = polygon.getExteriorRing.getCoordinates.distinct
      val coords = if(!CGAlgorithms.isCCW(ring)) {
        ring.reverse :+ ring.last
      } else {
        ring :+ ring.head
      }

      coords.zip(coords.tail).zipWithIndex.map{ case(coords, edgeId) =>
        val arr = Array(coords._1, coords._2)
        val edge = geofactory.createLineString(arr)
        val data = EdgeData(polygonId, 0, edgeId, false, label)
        edge.setUserData(data)
        Half_edge(edge)
      }
    }.toList.filter(_.data.polygonId == id)
    val h_prime = h :+ h.head
    h_prime.zip(h_prime.tail).foreach{ case(h1, h2) =>
      h1.next = h2
      h2.prev = h1
    }
    buffer.close

    h
  }

  private def saveHedges(name:String, hedges: List[Half_edge], tag: String = ""): Unit = {
    val hf = new java.io.PrintWriter(name)
    val wkt = hedges.map{ hedge =>
      val wkt = hedge.wkt
      val data = hedge.data

      s"$wkt\t$data\n"
    }
    hf.write(wkt.mkString(""))
    hf.close
    println(s"Saved $name [${wkt.size} records].")

  }
}
