package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.geom.Polygon
import com.vividsolutions.jts.io.WKTReader
import com.vividsolutions.jts.algorithm.CGAlgorithms

import edu.ucr.dblab.sdcel.geometries.{Half_edge, Vertex, EdgeData, HEdge}
import edu.ucr.dblab.sdcel.DCELMerger2.merge2
import scala.io.Source

object Test3 {
  def main(args: Array[String]): Unit = {
    val model = new PrecisionModel(1000000)
    implicit val geofactory = new GeometryFactory(model)
    implicit val reader = new WKTReader(geofactory)

    //val ha = readWKT2(args(0), "A", 225)
    val ha = readWKT(args(0), "A")
    saveHedges("/tmp/edgesHA.wkt", ha)

    //val hb = readWKT2(args(1), "B", 99)
    val hb = readWKT(args(1), "B")
    saveHedges("/tmp/edgesHB.wkt", hb)

    // Get merge half-edges...
    val h = merge2(ha, hb, true)

    val name = "/tmp/edgesH.wkt"
    val hf = new java.io.PrintWriter(name)
    val wkt = h.map{ case(h, tag) =>
      val wkt = h.getPolygon.toText

      s"$wkt\t$tag\n"
    }
    hf.write(wkt.mkString(""))
    hf.close
    println(s"Saved $name [${wkt.size} records].")

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