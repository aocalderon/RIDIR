package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.geom.Polygon
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
    val ha = bufferA.getLines.zipWithIndex.flatMap{ case(line, polygonId) =>
      val arr = line.split("\t")
      val polygon = reader.read(arr(0)).asInstanceOf[Polygon]
      val ring = polygon.getExteriorRing
      val coords = ring.getCoordinates.distinct :+ ring.getCoordinates.head

      coords.zip(coords.tail).zipWithIndex.map{ case(coords, edgeId) =>
        val arr = Array(coords._1, coords._2)
        val edge = geofactory.createLineString(arr)
        val data = EdgeData(polygonId, 0, edgeId, false, "A")
        edge.setUserData(data)
        Half_edge(edge)
      }
    }.toList
    val ha_prime = ha :+ ha.head
    ha_prime.zip(ha_prime.tail).foreach{ case(h1, h2) =>
      h1.next = h2
      h2.prev = h1
    }
    bufferA.close
    saveHedges("/tmp/edgesHA.wkt", ha)

    val bufferB = Source.fromFile(args(1))
    val hb = bufferB.getLines.zipWithIndex.flatMap{ case(line, polygonId) =>
      val arr = line.split("\t")
      val polygon = reader.read(arr(0)).asInstanceOf[Polygon]
      val ring = polygon.getExteriorRing
      val coords = ring.getCoordinates.distinct :+ ring.getCoordinates.head

      coords.zip(coords.tail).zipWithIndex.map{ case(coords, edgeId) =>
        val arr = Array(coords._1, coords._2)
        val edge = geofactory.createLineString(arr)
        val data = EdgeData(polygonId, 0, edgeId, false, "B")
        edge.setUserData(data)
        Half_edge(edge)
      }
    }.toList
    val hb_prime = hb :+ hb.head
    hb_prime.zip(hb_prime.tail).foreach{ case(h1, h2) =>
      h1.next = h2
      h2.prev = h1
    }
    bufferB.close
    saveHedges("/tmp/edgesHB.wkt", hb)

    // Get merge half-edges...
    val h = merge(ha, hb)

    val name = "/tmp/edgesH.wkt"
    val hf = new java.io.PrintWriter(name)
    val wkt = h.take(5).map{ hedge =>
      val wkt = hedge._2.getNextsAsWKT
      val pid = hedge._1

      s"$wkt\t$pid\n"
    }
    hf.write(wkt.mkString(""))
    hf.close
    println(s"Saved $name [${wkt.size} records].")

  }

  private def saveHedges(name:String, hedges: List[Half_edge], tag: String = ""): Unit = {
    val hf = new java.io.PrintWriter(name)
    val wkt = hedges.map{ hedge =>
      val wkt = hedge.edge.toText
      val pid = hedge.data.polygonId

      s"$wkt\t$pid\n"
    }
    hf.write(wkt.mkString(""))
    hf.close
    println(s"Saved $name [${wkt.size} records].")

  }
}
