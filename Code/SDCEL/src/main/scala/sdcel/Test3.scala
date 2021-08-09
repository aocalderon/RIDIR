package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.geom.{Polygon, LineString, Coordinate}
import com.vividsolutions.jts.io.WKTReader
import com.vividsolutions.jts.algorithm.CGAlgorithms

import edu.ucr.dblab.sdcel.geometries.{Half_edge, Vertex, EdgeData, HEdge}
import edu.ucr.dblab.sdcel.DCELMerger2.merge2
import edu.ucr.dblab.sdcel.DCELOverlay2.{overlay3, overlay4}
import scala.io.Source

object Test3 {
  def main(args: Array[String]): Unit = {
    val model = new PrecisionModel(1000)
    implicit val geofactory = new GeometryFactory(model)
    implicit val reader = new WKTReader(geofactory)

    println("In Test3")
    val c1 = new Coordinate(5,3)
    val c2 = new Coordinate(0,3)
    val c3 = new Coordinate(0,1)
    val c4 = new Coordinate(0,0)
    val c5 = new Coordinate(2,0)
    val c6 = new Coordinate(2,1)
    val c7 = new Coordinate(4,2)
    val c8 = new Coordinate(4,0)
    val c9 = new Coordinate(6,0)
    val c10 = new Coordinate(6,2)
    val c11 = new Coordinate(6,3)

    def createHEdge(c1: Coordinate, c2: Coordinate): Half_edge = {
      val e = geofactory.createLineString(Array(c1,c2))
      val ed = EdgeData(1,0,1,false)
      e.setUserData(ed)
      val h = Half_edge(e)
      h
    }

    val h1 = createHEdge(c1, c2)
    val h2 = createHEdge(c2, c3)
    val h3 = createHEdge(c3, c4)

    val e4 = geofactory.createLineString(Array(c4,c5))
    val ed4 = EdgeData(-1,0,1,false)
    e4.setUserData(ed4)
    val h4 = Half_edge(e4)

    val h5 = createHEdge(c5, c6)
    val h6 = createHEdge(c6, c7)
    val h7 = createHEdge(c7, c8)

    val e8 = geofactory.createLineString(Array(c8,c9))
    val ed8 = EdgeData(1,0,1,false)
    ed8.setCellBorder(true)
    e8.setUserData(ed8)
    val h8 = Half_edge(e8)

    val h9 = createHEdge(c9, c10)
    val h10 = createHEdge(c10, c11)
    val h11 = createHEdge(c11, c1)

    /*
    h1.next = h2; h2.prev = h1
    h2.next = h3; h3.prev = h2
    h3.next = h4; h4.prev = h3
    h4.next = h5; h5.prev = h4
    h5.next = h6; h6.prev = h5
    h6.next = h7; h7.prev = h6
    h7.next = h8; h8.prev = h7
    h8.next = h9; h9.prev = h8
    h9.next = h10;  h10.prev = h9
    h10.next = h11; h11.prev = h10
    h11.next = h1;  h1.prev =  h11
     */

    val list = List(h1, h2, h3, h4, h5, h6, h7, h8, h9, h10, h11)
    //val list = List(h6, h10)
    list.foreach{ hh =>
      h1.next = h2; h2.prev = h1
      h2.next = h3; h3.prev = h2
      h3.next = h4; h4.prev = h3
      h4.next = h5; h5.prev = h4
      h5.next = h6; h6.prev = h5
      h6.next = h7; h7.prev = h6
      h7.next = h8; h8.prev = h7
      h8.next = h9; h9.prev = h8
      h9.next = h10;  h10.prev = h9
      h10.next = h11; h11.prev = h10
      h11.next = h1;  h1.prev =  h11
      println(s"run ${hh.edge}")
      val t = (hh, "A0B0")
      val it = List(t)

      //overlay3(it).map{ case(h,l) => s"${h.getNextsAsWKT}\t$l" }.foreach(println)

      overlay4(it)
        .map{ case(s, e, l) => s"${s.edge.toText}\t${e.edge.toText}\t$l" }.foreach(println)
    }


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
