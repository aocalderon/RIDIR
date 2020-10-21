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
      .groupBy(_._1).mapValues(_.map(_._2))
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
    a1.setUserData(EdgeData(1,0,0,false))
    a2.setUserData(EdgeData(1,0,1,false))
    a3.setUserData(EdgeData(1,0,2,false))
    a4.setUserData(EdgeData(1,0,3,false))
    val ha = List(a1,a2,a3,a4).map{Half_edge}

    val b1 = geofactory.createLineString(Array(new Coordinate(1,1), new Coordinate(3,1)))
    val b2 = geofactory.createLineString(Array(new Coordinate(3,1), new Coordinate(3,3)))
    val b3 = geofactory.createLineString(Array(new Coordinate(3,3), new Coordinate(1,3)))
    val b4 = geofactory.createLineString(Array(new Coordinate(1,3), new Coordinate(1,1)))
    b1.setUserData(EdgeData(2,0,0,false))
    b2.setUserData(EdgeData(2,0,1,false))
    b3.setUserData(EdgeData(2,0,2,false))
    b4.setUserData(EdgeData(2,0,3,false))
    val hb = List(b1,b2,b3,b4).map{Half_edge}

    val intersections = intersects(ha, hb)

    val intersectionsCoords = intersections.keySet.map{geofactory.createPoint}

    val (ha1, ha2) = ha.partition{ a =>
      intersectionsCoords.exists(i => a.edge.intersects(i))
    }
    val (hb1, hb2) = hb.partition{ b =>
      intersectionsCoords.exists(i => b.edge.intersects(i))
    }

    intersections.foreach{ println }

    val f1 = new java.io.PrintWriter("/tmp/edgesI.wkt")
    f1.write(intersectionsCoords.map(_.toText).mkString("\n"))
    f1.close

    val af1 = new java.io.PrintWriter("/tmp/edgesA1.wkt")
    af1.write(
      ha1.map{ h =>
        val wkt = h.edge.toText
        val pid = h.data.polygonId
        val eid = h.data.edgeId
        s"$wkt\t$pid:$eid\n"
      }.mkString("")
    )
    af1.close
    val af2 = new java.io.PrintWriter("/tmp/edgesA2.wkt")
    af2.write(
      ha2.map{ h =>
        val wkt = h.edge.toText
        val pid = h.data.polygonId
        val eid = h.data.edgeId
        s"$wkt\t$pid:$eid\n"
      }.mkString("")
    )
    af2.close

    val bf1 = new java.io.PrintWriter("/tmp/edgesB1.wkt")
    bf1.write(
      hb1.map{ h =>
        val wkt = h.edge.toText
        val pid = h.data.polygonId
        val eid = h.data.edgeId
        s"$wkt\t$pid:$eid\n"
      }.mkString("")
    )
    bf1.close
    val bf2 = new java.io.PrintWriter("/tmp/edgesB2.wkt")
    bf2.write(
      hb2.map{ h =>
        val wkt = h.edge.toText
        val pid = h.data.polygonId
        val eid = h.data.edgeId
        s"$wkt\t$pid:$eid\n"
      }.mkString("")
    )
    bf2.close
    
  }
}
