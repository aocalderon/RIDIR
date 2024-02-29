package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geom._
import edu.ucr.dblab.sdcel.DCELMerger2.{groupByNextMBRPoly, setTwins}
import edu.ucr.dblab.sdcel.Utils._
import edu.ucr.dblab.sdcel.geometries._
import edu.ucr.dblab.sdcel.quadtree._
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.slf4j.Logger

import scala.annotation.tailrec
import scala.collection.JavaConverters._

object LocalDCEL {
  def createLocalDCELs(edgesRDD: RDD[LineString], letter: String = "A")
    (implicit cells: Map[Int, Cell], geofactory: GeometryFactory, settings: Settings)
      : RDD[(Half_edge, String, Envelope, Polygon)] = {

    val r = edgesRDD.mapPartitionsWithIndex{ (pid, it) =>
      val cell = cells(pid)

      val (containedIt, crossingIt) = classifyEdges(it)
      val crossing = getCrossing(crossingIt.toList).filter(_.getLength > 0)
      val S = getIntersectionsOnBorder(crossing, "S")
      val W = getIntersectionsOnBorder(crossing, "W")
      val N = getIntersectionsOnBorder(crossing, "N")
      val E = getIntersectionsOnBorder(crossing, "E")
      val bordersS = splitBorder(cell.getSouthBorder, S)
      val bordersE = splitBorder(cell.getEastBorder, E)
      val bordersN = splitBorder(cell.getNorthBorder, N)
      val bordersW = splitBorder(cell.getWestBorder, W)

      val borders = (bordersS ++ bordersE ++ bordersN ++ bordersW).map{ b => Half_edge(b) }
      val inners  = (containedIt ++ crossing).toList.map{ i => Half_edge(i) }

      setTwins(borders)
      setNextAndPrevBorders(borders)

      setTwins(inners)

      val inner_segments = sortInnerHedges(inners)
      setNextAndPrev(inner_segments)
      val (inner_closed, inner_open) = inner_segments.partition(_.isClose)
      // closing pointer in polygons fully contained...
      inner_closed.map{ seg =>
        seg.last.next = seg.first
        seg.first.prev = seg.last
      }

      (inners ++ borders).zipWithIndex
      .map{ case(hedge, id) =>
        hedge.id = id
        hedge
      }

      merge(inners.filter(_.data.crossingInfo != "None").filter(_.twin != null),  borders)
      
      val hedges = groupByNextMBRPoly((inners).toSet,
        List.empty[(Half_edge, String, Envelope, Polygon)])

      hedges.filter(_._2.split(" ").size == 1).toIterator
    }
    if(TaskContext.getPartitionId() == 3) {
      r.mapPartitionsWithIndex { (pid, hedges) =>
        hedges.map { case (h, l, e, p) =>
          val wkt = p.toText
          s"$wkt\t$pid"
        }
      }.foreach{println}
    }

    r
  }

  def merge(crossing0: List[Half_edge], borders0: List[Half_edge])
    (implicit logger: Logger): Unit = {
    val crossing = crossing0 ++ crossing0.filter(_.twin.isNewTwin).map(_.twin)
    val vertices = borders0.map(_.v2)
    val borders =  borders0 ++ borders0.filter(_.twin.isNewTwin).map(_.twin)

    val hedges_prime = (borders ++ crossing)
    val hedges = hedges_prime.filter(h => vertices.contains(h.v2))
    val incidents = hedges.groupBy(_.v2).values.toList

    // At each vertex, get their incident half-edges...
    val h_prime = incidents.filter(_.size > 1).map{ hList =>
      // Sort them by angle...
      val hs = hList.sortBy(- _.angleAtDest)
      // Add first incident to complete the sequence...
      val hs_prime = hs :+ hs.head
      // zip and tail will pair each half-edge with its next one...
      hs_prime.zip(hs_prime.tail).foreach{ case(h1, h2) =>
        h1.next = h2.twin
        h2.twin.prev = h1
      }

      hs
    }
  }

  def sortInnerHedges(innerHedges: List[Half_edge]): List[Segment] = {
    val segs = innerHedges.groupBy{in => (in.data.polygonId, in.data.ringId)}
      .flatMap{ case(pid, hedges_prime) =>
        val hedges = hedges_prime.sortBy(_.data.edgeId).toList

        val ins = getLineSegments(hedges.tail, List(hedges.head),
          List.empty[List[Half_edge]])

        val segs = ins.map{ hs => Segment(hs) }
        if(segs.head.startId == 0){
          // If the first edge Id of the polygon is here, we have to check the continuity...
          val start = segs.head
          val exts = segs.tail.filter( t => start.first.v1 == t.last.v2 )
          if(exts.isEmpty){
            segs
          } else {
            val head = segs.head
            val last = segs.last
            val new_head = Segment(last.hedges ++ head.hedges)
            new_head +: segs.slice(1, segs.length - 1)
          }
        } else {
          segs
        }
      }.toList

    segs
  }

  @tailrec
  def getLineSegments(hedges: List[Half_edge], segment: List[Half_edge],
    segments: List[List[Half_edge]]): List[List[Half_edge]] = {

    hedges match {
      case Nil => segments :+ segment
      case head +: tail => {
        val (new_current, new_segments) = {
          val prev = segment.last.data.edgeId
          val next = head.data.edgeId
          if( prev + 1 == next ){
            (segment :+ head, segments)
          }
          else (List(head), segments :+ segment)
        }
        getLineSegments(tail, new_current, new_segments)
      }
    }
  }
  
  // Set the next and prev pointer for the inners edges based on its edgeId...
  def setNextAndPrev(segs: List[Segment]): Unit = {
    segs.map{ seg =>
      val hedges = seg.hedges
      hedges.zip(hedges.tail).map{ case(h1, h2) =>
        h1.next = h2
        h2.prev = h1
      }
    }
  }

  // Set the next and prev pointer for the inners edges based on its edgeId...
  def setNextAndPrevBorders(hedges: List[Half_edge]): Unit = {
    hedges.zip(hedges.tail).map{ case(h1, h2) =>
      h1.next = h2
      h2.prev = h1
    }
    hedges.head.prev = hedges.last
    hedges.last.next = hedges.head
  }

  // Split crossing edges according to the cell border they intersect...
  // Return them as half-edges to keep information about the original edge...
  def getCrossing(crossing: List[LineString])
    (implicit geofactory: GeometryFactory): List[LineString] = {

    crossing
      .filter{ edge =>
        // Remove edges that touch the same border twice...
        // They will be repeated in the borders edges...
        val crossing_info = getCrossingInfo(edge).split("\\|")
        val borders = crossing_info.map(_.split(":")(0)).toList
        val borders_dist = borders.distinct
        borders.size == borders_dist.size
      }
      .map{ edge =>
      val crossing_info = getCrossingInfo(edge).split("\\|")
      val nsplits = crossing_info.size

      nsplits match {
        // Single case, the edge has only one intersection.
        case 1 => {
          val arr = crossing_info.head.split(":")
          val border = arr(0)
          val xy = arr(1).split(" ") 
          val coord = new Coordinate(xy(0).toDouble, xy(1).toDouble)
          val (split, invalidVertex) = splitEdge2(edge, border, coord)
          val data = edge.getUserData.asInstanceOf[EdgeData]
          data.setWKT(edge.toText)
          data.setInvalidVertex(invalidVertex)
          split.setUserData(data)
          
          split
        }
        case _ => {
          // If the edge has two different intersections we extract the section between
          // those two coords.  We sorted the coords according how close they are from the
          // start of the edge.
          val intersections = crossing_info.map{ cross =>
            val data = cross.split(":")
            val xy = data(1).split(" ")
            new Coordinate(xy(0).toDouble, xy(1).toDouble)
          }.sortBy{_.distance(edge.getStartPoint.getCoordinate)}
          val c1 = intersections(0)
          val c2 = intersections(1)
          val (split, invalidVertex) = if(c1 != c2){
            val split = geofactory.createLineString(Array(c1, c2))
            (split, 3) // Both coordinates are invalid
          } else {
            // If the two coords are the same is because the edge touch just a corner.
            // We select a coord and solve it as an edge with a single intersection point.
            val arr = crossing_info.head.split(":")
            val border = arr(0)
            val xy = arr(1).split(" ")
            val coord = new Coordinate(xy(0).toDouble, xy(1).toDouble)
            val (split, invalidVertex) = splitEdge2(edge, border, c1)
            (split, invalidVertex)
          }

          val data = edge.getUserData.asInstanceOf[EdgeData]
          data.setWKT(edge.toText)
          data.setInvalidVertex(invalidVertex)
          split.setUserData(data)
          split
        }
      }
    }
  }

  // Return the section of a edge crossing a border depending on its orientation...
  def splitEdge(edge: LineString, border: String, coord: Coordinate)
    (implicit geofactory: GeometryFactory): LineString = {

    val start = edge.getStartPoint.getCoordinate
    val end   = edge.getEndPoint.getCoordinate
    
    // Use the border side to infer the orientation...
    // for example, if edge intersects north we ask if the start of the edge is below
    // or above that intersection.
    val coords = border match {
      case "N" => {
        if (start.y > coord.y)
          Array(coord, end)
        else if(start.y < coord.y)
          Array(start, coord)
        else {
          if(end.y < coord.y)
            Array(coord, end)
          else
            Array(coord, coord)
        }
      }
      case "S" => {
        if (start.y < coord.y)
          Array(coord, end)
        else if(start.y > coord.y)
          Array(start, coord)
        else{
          if(end.y > coord.y)
            Array(coord, end)
          else
            Array(coord, coord)
        }
      }
      case "W" => {
        if (start.x < coord.x)
          Array(coord, end)
        else if(start.x > coord.x)
          Array(start, coord)
        else {
          if(end.x > coord.x)
            Array(coord, end)
          else
            Array(coord, coord)
        }
      }
      case "E" => {
        if (start.x > coord.x)
          Array(coord, end)
        else if(start.x < coord.x)
          Array(start, coord)
        else {
          if(end.x < coord.x)
            Array(coord, end)
          else
            Array(coord, coord)
        }
      }
    }

    geofactory.createLineString(coords)
  }

  // Return the section of a edge crossing a border depending on its orientation...
  def splitEdge2(edge: LineString, border: String, coord: Coordinate)
    (implicit geofactory: GeometryFactory): (LineString, Int) = {

    val start = edge.getStartPoint.getCoordinate
    val end   = edge.getEndPoint.getCoordinate
    
    // Use the border side to infer the orientation...
    // for example, if edge intersects north we ask if the start of the edge is below
    // or above that intersection.
    val (coords, invalidVertices) = border match {
      case "N" => {
        if (start.y > coord.y)
          (Array(coord, end), 1)
        else if(start.y < coord.y)
          (Array(start, coord), 2)
        else {
          if(end.y < coord.y)
            (Array(coord, end), 0)
          else
            (Array(coord, coord), 3)
        }
      }
      case "S" => {
        if (start.y < coord.y)
          (Array(coord, end), 1)
        else if(start.y > coord.y)
          (Array(start, coord), 2)
        else{
          if(end.y > coord.y)
            (Array(start, end), 0)
          else
            (Array(coord, coord), 3)
        }
      }
      case "W" => {
        if (start.x < coord.x)
          (Array(coord, end), 1)
        else if(start.x > coord.x)
          (Array(start, coord), 2)
        else {
          if(end.x > coord.x)
            (Array(start, end),0)
          else
            (Array(coord, coord), 3)
        }
      }
      case "E" => {
        if (start.x > coord.x)
          (Array(coord, end),1)
        else if(start.x < coord.x)
          (Array(start, coord),2)
        else {
          if(end.x < coord.x)
            (Array(start, end),0)
          else
            (Array(coord, coord),3)
        }
      }
    }

    (geofactory.createLineString(coords), invalidVertices)
  }

  // Split a border by a list of coordinates...
  def splitBorder(border: LineString, coords: List[Coordinate])
      (implicit geofactory: GeometryFactory): List[LineString] = {

    @tailrec
    // Recursively cut the first section of the coord at the head of the list.
    // Note: the coords have been sorted according to the border.
    def splitBorderT(border: LineString, coords: List[Coordinate], r: List[LineString])
      (implicit geofactory: GeometryFactory): List[LineString] = {

      coords match {
        case Nil => r
        case x :: tail => {
          val start = border.getStartPoint.getCoordinate
          val end = border.getEndPoint.getCoordinate
          val split1 = geofactory.createLineString(Array(start, x))
          val split2 = geofactory.createLineString(Array(x, end))

          val new_r = r :+ split1
          splitBorderT(split2, tail, new_r)
        }
      }
    }

    // Handling special cases: ensuring than end point border is in coords list and removing start point to avoid splits with no length...
    if(coords.isEmpty){
      List(border)
    } else {
      val end = border.getEndPoint.getCoordinate
      val start = border.getStartPoint.getCoordinate
      val coords2 = if(coords.last != end) coords :+ end else coords
      val coords3 = if(coords2.head == start) coords2.tail else coords2

      splitBorderT(border, coords3, List.empty[LineString])
    }
  }

  // Return list of coordinate intersection on a particular border (S,E,N or W)...
  def getIntersectionsOnBorder(edges: List[LineString], border: String):
      List[Coordinate] = {

    val intersections = edges.filter(e => getCrossingInfo(e).contains(s"${border}:"))
      .flatMap{ edge =>
        val cross  = getCrossingInfo(edge)
        // The format for the crossing info is: "N:x1 y1|W:x2 y2"
        val coords = cross.split("\\|").filter(_.contains(border)).map{ data =>
          val string_coords = data.split(":")(1)
          val xy = string_coords.split(" ")
          new Coordinate(xy(0).toDouble, xy(1).toDouble)
        }.toList
        coords
      }.distinct

    border match {
      case "S" => intersections.sortBy(_.x)
      case "E" => intersections.sortBy(_.y)
      case "N" => intersections.sortBy(_.x)(Ordering[Double].reverse)
      case "W" => intersections.sortBy(_.y)(Ordering[Double].reverse)
    }
  }

  // Partition the edges on fully contained and crossing a border...
  def classifyEdges(edges: Iterator[LineString]):
      (Iterator[LineString], Iterator[LineString]) = {

    edges.partition(edge => getCrossingInfo(edge) == "None")
  }

  // Return the field with the crossing info for a particular edge...
  private def getCrossingInfo(edge: LineString): String = {
    val data = edge.getUserData.asInstanceOf[EdgeData]
    data.crossingInfo
  }
  // Return the number of edges of the polygon for this edge...
  private def getNEdges(edge: LineString): Int = {
    val data = edge.getUserData.asInstanceOf[EdgeData]
    data.nedges
  }
}
