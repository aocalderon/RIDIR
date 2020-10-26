package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geomgraph.index.SimpleMCSweepLineIntersector
import com.vividsolutions.jts.geomgraph.index.SegmentIntersector
import com.vividsolutions.jts.algorithm.RobustLineIntersector
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.geom.Coordinate
import com.vividsolutions.jts.geomgraph.EdgeIntersection
import com.vividsolutions.jts.geomgraph.Edge
import scala.collection.JavaConverters._
import edu.ucr.dblab.sdcel.geometries.{Half_edge, EdgeData}
import edu.ucr.dblab.sdcel.DCELMerger2.setTwins

object SweepLine3 {

  case class HEdge(coords: Array[Coordinate], h: Half_edge) extends Edge(coords)

  def intersects(hedgesA: List[Half_edge], hedgesB: List[Half_edge],
    partitionId: Int = -1)
    (implicit geofactory: GeometryFactory): Map[Coordinate, List[Half_edge]] = {

    val aList = hedgesA.map{ h =>
      val pts = Array(h.v1, h.v2)
      HEdge(pts, h)
    }.asJava

    val bList = hedgesB.map{ h =>
      val pts = Array(h.v1, h.v2)
      HEdge(pts, h)
    }.asJava
    
    val sweepline = new SimpleMCSweepLineIntersector()
    val lineIntersector = new RobustLineIntersector()
    lineIntersector.setMakePrecise(geofactory.getPrecisionModel)
    val segmentIntersector = new SegmentIntersector(lineIntersector, true, true)

    sweepline.computeIntersections(aList, bList, segmentIntersector)

    (getIntersections(aList) ++ getIntersections(bList))
      .groupBy(_._1).mapValues(_.map(_._2).sortBy(_.data.label))
  }

  private def getIntersections(list: java.util.List[HEdge]):
      List[(Coordinate, Half_edge)] = {
    list.asScala.flatMap{ edge =>
      edge.getEdgeIntersectionList.iterator.asScala.map{ i =>
        (i.asInstanceOf[EdgeIntersection].getCoordinate, edge.h)
      }.toList
    }.toList
  }

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
    /********************************************************/

    /********************************************************/
    val intersections = intersects(ha, hb)
    val splits = intersections.flatMap{ case(p, hs) =>
      val h1 = hs.head
      val h2 = hs.last

      h1.split(p) ++ h2.split(p)
    }
    val hedges = setTwins(splits.toList)

    /* debugging
    def printH(h: Half_edge, hid: Int, t: String): String =
      s"${h.edge.toText}\t${hid}\t${t}\t${h.label}"
    def printt(h: Half_edge, hid: Int): String = {
      printH(h.prev, hid, "P") + "\n" +
      printH(h, hid, "C") + "\n" +
      printH(h.twin, hid, "T") + "\n" +
      printH(h.next, hid, "N")
    }
    val hh1 = hedges.zipWithIndex.map{case(h, hid)=>printt(h,hid)}
     */

    val incidents = (hedges ++ hedges.map(_.twin)) // Take half-edges and their twins...
      .groupBy(_.v2) // Group them by the destination vertex...
      .filter(_._2.size > 1) // Remove isolate vertices
                             // (those with less than 2 incident half-edges)

    // At each vertex, get their incident half-edges...
    val hh = incidents.mapValues{ hList =>
      // Sort them by angle...
      val hs = hList.sortBy(- _.angleAtDest)
      // Add first incident to comple the sequence...
      val hs_prime = hs :+ hs.head
      // zip and tail will pair each half-edge with its next one...
      hs_prime.zip(hs_prime.tail).foreach{ case(h1, h2) => 
        h1.next = h2.twin
        h2.twin.prev = h1
      }

      hs
    }.values.flatten

    val h = hh.map{ h =>
      val pid1 = h.data.polygonId
      val pid2 = h.next.data.polygonId
      if( pid1 >= 0 && pid2 <  0 ) (h.label, h)
      else if( pid1 <  0 && pid2 >= 0 ) (h.next.label, h.next)
      else if( pid1 >= 0 && pid2 >= 0 )
        (List(h.label, h.next.label).sorted.distinct.mkString(" "), h)
      else ("", null)
    }.filterNot(_._1 == "")
      .groupBy(_._1).values.map(_.head)
    /********************************************************/

    /********************************************************/

    val intersectionsPoints = intersections.keySet.map{geofactory.createPoint}
    val vif = new java.io.PrintWriter("/tmp/edgesVi.wkt")
    vif.write(intersectionsPoints.map(_.toText).mkString("\n"))
    vif.close

    val hf = new java.io.PrintWriter("/tmp/edgesH.wkt")
    hf.write(
      h.map{ x =>
        val wkt = x._2.getPolygon.toText
        val label = x._1

        s"$wkt\t$label\n"
      }.mkString("")
    )
    hf.close
    
    /********************************************************/
  }
}
