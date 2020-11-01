package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.geom.LineString
import com.vividsolutions.jts.io.WKTReader
import edu.ucr.dblab.sdcel.geometries.{Half_edge, Vertex, EdgeData, HEdge}
import edu.ucr.dblab.sdcel.DCELMerger2.merge
import scala.io.Source

object Test3 {
  def main(args: Array[String]): Unit = {
    val model = new PrecisionModel(1000)
    implicit val geofactory = new GeometryFactory(model)
    implicit val reader = new WKTReader(geofactory)

    val bufferA = Source.fromFile(args(0))
    val ha = bufferA.getLines.map{ line =>
      val arr = line.split("\t")
      val linestring = reader.read(arr(0)).asInstanceOf[LineString]
      val polygonId = arr(2).toInt
      val ringId = arr(3).toInt
      val edgeId = arr(4).toInt
      val isHole = arr(5).toBoolean
      val data = EdgeData(polygonId, ringId, edgeId, isHole, "A")
      linestring.setUserData(data)
      Half_edge(linestring)
    }.toList.distinct
    ha.groupBy(_.label).mapValues{ h_prime => 
      val h = h_prime.sortBy(_.data.edgeId)
      h.zip(h.tail).foreach{ case(h1, h2) =>
        h1.next = h2
        h2.prev = h1
      }
      h.head.prev = h.last
      h.last.next = h.head
    }
    bufferA.close

    val bufferB = Source.fromFile(args(1))
    val hb = bufferB.getLines.map{ line =>
      val arr = line.split("\t")
      val linestring = reader.read(arr(0)).asInstanceOf[LineString]
      val polygonId = arr(2).toInt
      val ringId = arr(3).toInt
      val edgeId = arr(4).toInt
      val isHole = arr(5).toBoolean
      val data = EdgeData(polygonId, ringId, edgeId, isHole, "B")
      linestring.setUserData(data)
      Half_edge(linestring)
    }.toList.distinct
    hb.groupBy(_.label).mapValues{ h_prime => 
      val h = h_prime.sortBy(_.data.edgeId)
      h.zip(h.tail).foreach{ case(h1, h2) =>
        h1.next = h2
        h2.prev = h1
      }
      h.head.prev = h.last
      h.last.next = h.head
    }
    bufferB.close

    // Get merge half-edges...
    val h = merge(ha, hb)

    val name = "/tmp/edgesH.wkt"
    val hf = new java.io.PrintWriter(name)
    val wkt = h.map{ x =>
      val wkt = x._2.getPolygon.toText
      val label = x._1

      s"$wkt\t$label\n"
    }
    hf.write(wkt.mkString(""))
    hf.close
    println(s"Saved $name [${wkt.size} records].")
  }
}
