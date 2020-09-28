package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geomgraph.index.{SimpleMCSweepLineIntersector, SegmentIntersector}
import com.vividsolutions.jts.algorithm.RobustLineIntersector
import com.vividsolutions.jts.geom.{PrecisionModel, Envelope, Coordinate}
import com.vividsolutions.jts.geom.GeometryFactory
import com.vividsolutions.jts.geomgraph.EdgeIntersection
import com.vividsolutions.jts.geom.{LinearRing, LineString, Polygon}
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
}
