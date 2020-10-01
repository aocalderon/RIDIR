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
import scala.annotation.tailrec
import edu.ucr.dblab.sdcel.geometries.{Vertex, Half_edge, EdgeData}

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

  /****************************************************************************************/

  object Mode  extends Enumeration {
    type Mode = Value
    val On, In, Out = Value
  }
  import Mode._

  case class ConnectedIntersection(coord: Coordinate,
    hins: List[Half_edge] = List.empty[Half_edge],
    houts: List[Half_edge] = List.empty[Half_edge]){

    var next: ConnectedIntersection = null
  }

  case class Intersection(coord: Coordinate, hedge: Half_edge, mode: Mode)

  def getHedgesTouchingCell(outerEdges: Vector[LineString], mbr: LinearRing,
    index: Int = -1)
    (implicit geofactory: GeometryFactory): Vector[List[Half_edge]] = {

    val edgesList = outerEdges.map{ linestring =>
      new com.vividsolutions.jts.geomgraph.Edge(linestring.getCoordinates)
    }.asJava

    val mbrList = mbr.getCoordinates.zip(mbr.getCoordinates.tail).toList.map{case(p1, p2) =>
      new com.vividsolutions.jts.geomgraph.Edge(Array(p1, p2))
    }.asJava

    val sweepline = new SimpleMCSweepLineIntersector()
    val lineIntersector = new RobustLineIntersector()
    lineIntersector.setMakePrecise(geofactory.getPrecisionModel)
    val segmentIntersector = new SegmentIntersector(lineIntersector, true, true)

    sweepline.computeIntersections(edgesList, mbrList, segmentIntersector)

    // Getting list of the intersections (coordinates) on cell...
    val iOn = getCoordsOn(mbrList).map{ con =>
      val hempty = Half_edge(null)
      Intersection(con, hempty, Mode.On) // we will not need hempty later...
    }

    // Getting hedges coming to the cell (in) and leaving the cell (out)...
    val (edgesIn, edgesOut) = getEdgesTouch(edgesList, outerEdges, mbr)
    val iIn = edgesIn.map{ edge =>
      val hin = Half_edge(edge)
      Intersection(hin.v2, hin, Mode.In) // hin comes to the cell at coord v2...
    }
    val iOut = edgesIn.map{ edge =>
      val hout = Half_edge(edge)
      Intersection(hout.v1, hout, Mode.Out) // hout leaves the cell at coord v1... 
    }

    // Grouping hedges in and out by their coordinate intersections...
    val intersections = iOn union iIn union iOut
    val ring = intersections.groupBy(_.coord).map{ case(coord, inters) =>
      val hins  = inters.filter(_.mode == In).map(_.hedge).toList
      val houts = inters.filter(_.mode == Out).map(_.hedge).toList

      ConnectedIntersection(coord, hins, houts)
    }
    // Creating the circular linked list over the coordinate intersections...
    ring.zip(ring.tail).foreach{ case(c1, c2) => c1.next = c2}
    ring.last.next = ring.head

    ???
  }
  // Getting list of coordinates which intersect the cell...
  private def getCoordsOn(mbrList: java.util.List[Edge]): Vector[Coordinate] = {
    mbrList.asScala.flatMap{ edge =>
      val start = edge.getCoordinates.head
      val inners = edge.getEdgeIntersectionList.iterator.asScala.map{ i =>
        i.asInstanceOf[EdgeIntersection].getCoordinate
      }.toVector

      start +: inners 
    }.toVector
  }
  
  /****************************************************************************************/

  def getEdgesTouchingCell(outerEdges: Vector[LineString], mbr: LinearRing, index: Int = -1)
    (implicit geofactory: GeometryFactory): Vector[List[Half_edge]] = {

    val edgesList = outerEdges.map{ linestring =>
      new com.vividsolutions.jts.geomgraph.Edge(linestring.getCoordinates)
    }.asJava

    val mbrList = mbr.getCoordinates.zip(mbr.getCoordinates.tail).toList.map{case(p1, p2) =>
      new com.vividsolutions.jts.geomgraph.Edge(Array(p1, p2))
    }.asJava

    val sweepline = new SimpleMCSweepLineIntersector()
    val lineIntersector = new RobustLineIntersector()
    lineIntersector.setMakePrecise(geofactory.getPrecisionModel)
    val segmentIntersector = new SegmentIntersector(lineIntersector, true, true)

    sweepline.computeIntersections(edgesList, mbrList, segmentIntersector)

    val edgesOn = getEdgesOn(mbrList, mbr)
    val (edgesIn, edgesOut) = getEdgesTouch(edgesList, outerEdges, mbr)

    val l = getHedges(edgesOn, edgesIn, edgesOut, index)
    if(index == 14){
      //l.foreach{println}
    }
    l
  }

  private def getHedges(edgesOn: Vector[LineString],
    edgesIn: Vector[LineString],
    edgesOut: Vector[LineString], index: Int = -1): Vector[List[Half_edge]] = {

    val hOn = edgesOn.map(on => Half_edge(on))
    hOn.zip(hOn.tail).foreach{ case(h1, h2) => h1.next = h2 }
    hOn.last.next = hOn.head

    val hins = edgesIn.map(h => Half_edge(h))
    val houts = edgesOut.map(h => Half_edge(h))

    val hInsOuts = getSortedPairs(hOn.head, hins, houts,
      Vector.empty[(Half_edge, Half_edge)])
    if(index == 14){
      //hInsOuts.foreach{println}
    }

    hInsOuts.map{ case(hIn, hOut) =>
      getSection(hOn.head, hIn.v2, hOut.v1)
    }
  }

  private def getSection(h: Half_edge, v2: Coordinate, v1: Coordinate): List[Half_edge] = {
    val r = List.empty[Half_edge]
    if(v2 == v1){
      r
    } else {
      val h1 = getCoordAtOrig(h, v2)
      val h2 = getCoordAtDest(h, v1)
      getStartEnd(h1, h2, r).map(_.copy())
    }
  }
  @tailrec
  private def getCoordAtOrig(h: Half_edge, orig: Coordinate): Half_edge = {
    if(h.v1 == orig) h else getCoordAtOrig(h.next, orig)
  }
  @tailrec
  private def getCoordAtDest(h: Half_edge, dest: Coordinate): Half_edge = {
    if(h.v2 == dest) h else getCoordAtDest(h.next, dest)
  }
  @tailrec
  private def getStartEnd(start: Half_edge, end: Half_edge,
    inner: List[Half_edge]): List[Half_edge] = {

    if(start == end){
      inner :+ end
    } else {
      getStartEnd(start.next, end, start +: inner)
    }
  }

  @tailrec
  private def getSortedPairs(hedge: Half_edge,
    hins: Vector[Half_edge],
    houts: Vector[Half_edge],
    hInsOuts: Vector[(Half_edge, Half_edge)]): Vector[(Half_edge, Half_edge)] = {

    if(hins.isEmpty){
      hInsOuts
    } else {
      val (hin, hins_remain, current1) = findNextIn(hedge, hins)
      val (hout, houts_remain, current2) = findNextOut(current1, houts)
      val hInOut = (hin, hout)
      println(hInOut)
      getSortedPairs(current2, hins_remain, houts_remain, hInsOuts :+ hInOut)
    }
  }
  @tailrec
  private def findNextIn(current: Half_edge,
    hins: Vector[Half_edge]): (Half_edge, Vector[Half_edge], Half_edge) = {

    val v1 = current.v1
    if(hins.exists(_.v2 == v1)){
      val hin = hins.filter(_.v2 == v1).head
      val hins_remain = hins.filterNot(_.v2 == v1)
      (hin, hins_remain, current)
    } else {
      findNextIn(current.next, hins)
    }
  }
  @tailrec
  private def findNextOut(current: Half_edge,
    houts: Vector[Half_edge]): (Half_edge, Vector[Half_edge], Half_edge) = {

    val v2 = current.v2
    if(houts.exists(_.v1 == v2)){
      val hout = houts.filter(_.v1 == v2).head
      val houts_remain = houts.filterNot(_.v1 == v2)
      (hout, houts_remain, current)
    } else {
      findNextOut(current.next, houts)
    }
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
      segment.setUserData(EdgeData(-1,0,id,false))
      segment
    }
  }

  private def getEdgesTouch(edgesList: java.util.List[Edge],
    outerEdges: Vector[LineString], mbr: LinearRing)
    (implicit geofactory: GeometryFactory): (Vector[LineString],Vector[LineString])  = {

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

    val in = edgesTouchIn.map(_._2).toVector
    val out = edgesTouchOut.map(_._2).toVector

    (in, out)
  }
}
