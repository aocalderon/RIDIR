package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geomgraph.index.SimpleMCSweepLineIntersector
import com.vividsolutions.jts.geomgraph.index.SegmentIntersector
import com.vividsolutions.jts.algorithm.RobustLineIntersector
import com.vividsolutions.jts.geom.{PrecisionModel, Envelope, Coordinate}
import com.vividsolutions.jts.geom.GeometryFactory
import com.vividsolutions.jts.geomgraph.EdgeIntersection
import com.vividsolutions.jts.geom.{LinearRing, LineString, Polygon, Point}
import com.vividsolutions.jts.geomgraph.Edge
import scala.collection.JavaConverters._
import edu.ucr.dblab.sdcel.geometries.EdgeData

object SweepLine2 {
  def getEdgesOnCell(outerEdges: Vector[LineString], mbr: LinearRing)
    (implicit geofactory: GeometryFactory)
      : Vector[LineString] = {

    val edgesList = outerEdges.map{ linestring =>
      new com.vividsolutions.jts.geomgraph.Edge(linestring.getCoordinates)
    }.asJava

    val mbrList = mbr.getCoordinates.zip(mbr.getCoordinates.tail)
      .toList.map{ case(p1, p2) =>
        new com.vividsolutions.jts.geomgraph.Edge(Array(p1, p2))
      }.asJava

    val sweepline = new SimpleMCSweepLineIntersector()
    val lineIntersector = new RobustLineIntersector()
    val segmentIntersector = new SegmentIntersector(lineIntersector, true, true)

    sweepline.computeIntersections(edgesList, mbrList, segmentIntersector)

    val edgesOnCell = mbrList.asScala.flatMap{ edge =>
      val extremes = edge.getCoordinates
      val start = extremes(0)
      val end = extremes(1)
      val inners = edge.getEdgeIntersectionList.iterator.asScala.map{ i =>
        i.asInstanceOf[EdgeIntersection].getCoordinate
      }.toArray

      val coords = start +: inners :+ end
      coords.zip(coords.tail).map{ case(p1, p2) =>
        val edge = geofactory.createLineString(Array(p1, p2))
        edge.setUserData(EdgeData(-1,0,0,false))
        edge
      }
    }.toVector

    val outerEdgesMap = outerEdges.map{ edge => edge.getCoordinates -> edge }.toMap
    val envelope = mbr.getEnvelopeInternal
    val edgesInsideCell = edgesList.asScala.flatMap{ edge =>
      val extremes = edge.getCoordinates
      val data = outerEdgesMap(extremes).getUserData
      val start = extremes(0)
      val end = extremes(1)

      val eiList = edge.getEdgeIntersectionList.iterator.asScala.toList
      if(eiList.length == 1){
        val intersection = eiList.head.asInstanceOf[EdgeIntersection].getCoordinate
        val coords = {
          if(start != intersection &&
            end != intersection &&
            envelope.contains(start) &&
            !envelope.contains(end)) Array(start, intersection)
          else if(start != intersection &&
            end != intersection &&
            !envelope.contains(start) &&
            envelope.contains(end)) Array(intersection, end)
          else if(start == intersection &&
            envelope.contains(end)) Array(intersection, end)
          else if(end == intersection &&
            envelope.contains(start)) Array(start, intersection)
          else Array.empty[Coordinate]
        }
        val segment = geofactory.createLineString(coords)
        segment.setUserData(data)
        Vector(segment)
      } else {
        Vector.empty[LineString]
      }
    }.toVector

    edgesOnCell union edgesInsideCell
  }

  def getEdgesTouchingCell(outerEdges: Vector[LineString], mbr: LinearRing)
    (implicit geofactory: GeometryFactory): Vector[LineString] = {

    val edgesList = outerEdges.map{ linestring =>
      new com.vividsolutions.jts.geomgraph.Edge(linestring.getCoordinates)
    }.asJava

    val mbrList = mbr.getCoordinates.zip(mbr.getCoordinates.tail).toList.map{case(p1, p2) =>
      new com.vividsolutions.jts.geomgraph.Edge(Array(p1, p2))
    }.asJava

    val sweepline = new SimpleMCSweepLineIntersector()
    val lineIntersector = new RobustLineIntersector()
    val segmentIntersector = new SegmentIntersector(lineIntersector, true, true)

    sweepline.computeIntersections(edgesList, mbrList, segmentIntersector)

    val edgesOn = getEdgesOn(mbrList, mbr)
    val (edgesIn, edgesOut) = getEdgesTouch(edgesList, outerEdges, mbr)

    ???
  }

  private def getEdgesOn(mbrList: java.util.List[Edge], mbr: LinearRing)
    (implicit geofactory: GeometryFactory): Vector[LineString] = {

    val verticesOnCell = mbrList.asScala.flatMap{ edge =>
      val start = edge.getCoordinates.head
      val inners = edge.getEdgeIntersectionList.iterator.asScala.map{ i =>
        i.asInstanceOf[EdgeIntersection].getCoordinate
      }.toArray

      start +: inners
    }.toVector :+ mbr.getCoordinates.head

    verticesOnCell.zip(verticesOnCell.tail).zipWithIndex.map{ case(coords, id) =>
      val p1 = coords._1
      val p2 = coords._2
      val segment = geofactory.createLineString(Array(p1, p2))
      segment.setUserData(id)
      segment
    }
  }

  private def getEdgesTouch(edgesList: java.util.List[Edge],
    outerEdges: Vector[LineString], mbr: LinearRing)
    (implicit geofactory: GeometryFactory): (Vector[LineString], Vector[LineString]) = {

    case class T(coords: Array[Coordinate], data: EdgeData, isIn: Boolean)
    val outerEdgesMap = outerEdges.map{ edge => edge.getCoordinates -> edge }.toMap
    val envelope = mbr.getEnvelopeInternal
    val (edgesTouchIn, edgesTouchOut) = edgesList.asScala.map{ edge =>
      val extremes = edge.getCoordinates
      val data = outerEdgesMap(extremes).getUserData.asInstanceOf[EdgeData]
      val start = extremes(0)
      val end = extremes(1)

      val eiList = edge.getEdgeIntersectionList.iterator.asScala.toList
      if(eiList.length == 1){
        val intersection = eiList.head.asInstanceOf[EdgeIntersection].getCoordinate
        if(start != intersection && end != intersection &&
          envelope.contains(start) && !envelope.contains(end))
          T(Array(start, intersection), data, true)
        else if(start != intersection && end != intersection &&
          !envelope.contains(start) && envelope.contains(end))
          T(Array(intersection, end), data, false)
        else if(end == intersection && envelope.contains(start))
          T(Array(start, intersection), data, true)
        else if(start == intersection && envelope.contains(end))
          T(Array(intersection, end), data, false)
        else
          T(Array.empty[Coordinate], data, false)
      } else {
        T(Array.empty[Coordinate], data, false)
      }
    }.filterNot(_.coords.isEmpty).map{ t =>
        val segment = geofactory.createLineString(t.coords)
        segment.setUserData(t.data)
        (t.isIn, segment)
    }.partition(_._1)

    (edgesTouchIn.map(_._2).toVector, edgesTouchOut.map(_._2).toVector)
  }
}
