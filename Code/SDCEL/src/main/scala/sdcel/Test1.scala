package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.geom.Coordinate
import edu.ucr.dblab.sdcel.geometries.{Half_edge, Vertex, EdgeData, HEdge}
import edu.ucr.dblab.sdcel.DCELMerger2.merge

object Test1 {
  def main(args: Array[String]): Unit = {
    val model = new PrecisionModel(1000)
    implicit val geofactory = new GeometryFactory(model)

    val a1 = geofactory.createLineString(Array(new Coordinate(0,0), new Coordinate(2,0)))
    val a2 = geofactory.createLineString(Array(new Coordinate(2,0), new Coordinate(2,2)))
    val a3 = geofactory.createLineString(Array(new Coordinate(2,2), new Coordinate(0,2)))
    val a4 = geofactory.createLineString(Array(new Coordinate(0,2), new Coordinate(0,0)))
    a1.setUserData(EdgeData(1,0,0,false,"A"))
    a2.setUserData(EdgeData(1,0,1,false,"A"))
    a3.setUserData(EdgeData(1,0,2,false,"A"))
    a4.setUserData(EdgeData(1,0,3,false,"A"))
    val ha = List(a1,a2,a3,a4).map{Half_edge}
    ha.zip(ha.tail).foreach{ case(h1, h2) =>
      h1.next = h2
      h2.prev = h1
    }
    ha.head.prev = ha.last
    ha.last.next = ha.head

    val b1 = geofactory.createLineString(Array(new Coordinate(1,1), new Coordinate(3,1)))
    val b2 = geofactory.createLineString(Array(new Coordinate(3,1), new Coordinate(3,3)))
    val b3 = geofactory.createLineString(Array(new Coordinate(3,3), new Coordinate(1,3)))
    val b4 = geofactory.createLineString(Array(new Coordinate(1,3), new Coordinate(1,1)))
    b1.setUserData(EdgeData(2,0,0,false,"B"))
    b2.setUserData(EdgeData(2,0,1,false,"B"))
    b3.setUserData(EdgeData(2,0,2,false,"B"))
    b4.setUserData(EdgeData(2,0,3,false,"B"))
    val hb = List(b1,b2,b3,b4).map{Half_edge}
    hb.zip(hb.tail).foreach{ case(h1, h2) =>
      h1.next = h2
      h2.prev = h1
    }
    hb.head.prev = hb.last
    hb.last.next = hb.head

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