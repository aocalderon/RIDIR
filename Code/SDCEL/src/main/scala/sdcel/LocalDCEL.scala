package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geom.{LineString, Point, Coordinate}
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.geomgraph.index.SimpleMCSweepLineIntersector
import com.vividsolutions.jts.geomgraph.index.SegmentIntersector
import com.vividsolutions.jts.algorithm.RobustLineIntersector
import com.vividsolutions.jts.geomgraph.EdgeIntersection
import com.vividsolutions.jts.geomgraph.Edge

import scala.collection.JavaConverters._
import scala.annotation.tailrec

import org.apache.spark.sql.SparkSession
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD

import org.slf4j.{Logger, LoggerFactory}

import edu.ucr.dblab.sdcel.quadtree._
import edu.ucr.dblab.sdcel.geometries._

import SweepLine2.{getHedgesInsideCell, getLineSegments}
import DCELMerger2.{setTwins, groupByNext}
import Utils._

object LocalDCEL {
  def createLocalDCELs(edgesRDD: RDD[LineString], cells: Map[Int, Cell])
    (implicit geofactory: GeometryFactory, logger: Logger, spark: SparkSession,
    settings: Settings)
      : RDD[(Half_edge, String)] = {

    val partitionId = 1

    val r = edgesRDD.mapPartitionsWithIndex{ (pid, it) =>
      
      val (containedIt, crossingIt) = classifyEdges(it)
      val crossing = getCrossing(crossingIt.toList)

      val S = getIntersectionsOnBorder(crossing, "S")
      val W = getIntersectionsOnBorder(crossing, "W")
      val N = getIntersectionsOnBorder(crossing, "N")
      val E = getIntersectionsOnBorder(crossing, "E")
      val cell = cells(pid)
      val bordersS = splitBorder(cell.getSouthBorder, S)
      val bordersW = splitBorder(cell.getWestBorder, W)
      val bordersN = splitBorder(cell.getNorthBorder, N)
      val bordersE = splitBorder(cell.getEastBorder, E)
      val borders = (bordersS ++ bordersW ++ bordersN ++ bordersE).map{Half_edge}
      setTwins(borders)
      setNextAndPrevBorders(borders)

      val inner = (containedIt ++ crossing).map{Half_edge}.toList
      setTwins(inner)

      val inner_segments = sortInnerHedges(inner)
      setNextAndPrev(inner_segments)
      val (inner_closed, inner_open) = inner_segments.partition(_.isClose)
      // closing pointer in polygons fully contained...
      inner_closed.map{ seg =>
        seg.last.next = seg.first
        seg.first.prev = seg.last
      }

      merge(inner.filter(_.data.crossingInfo != "None"),  borders)
      
      if(pid == partitionId){
        val (containedIt, crossingIt) = classifyEdges(it)
        val crossing = getCrossing(crossingIt.toList)

        val S = getIntersectionsOnBorder(crossing, "S")
        val W = getIntersectionsOnBorder(crossing, "W")
        val N = getIntersectionsOnBorder(crossing, "N")
        val E = getIntersectionsOnBorder(crossing, "E")
        val cell = cells(pid)
        val bordersS = splitBorder(cell.getSouthBorder, S)
        val bordersW = splitBorder(cell.getWestBorder, W)
        val bordersN = splitBorder(cell.getNorthBorder, N)
        val bordersE = splitBorder(cell.getEastBorder, E)
        val borders = (bordersS ++ bordersW ++ bordersN ++ bordersE).map{Half_edge}
        setTwins(borders)
        setNextAndPrevBorders(borders)

        val inner = (containedIt ++ crossing).map{Half_edge}.toList
        setTwins(inner)

        
        val inner_segments = sortInnerHedges(inner)
        //println("Segments")
        inner_segments.map{ seg =>
          val ids = seg.hedges.map(_.data.edgeId).mkString(" ")
          println(s"${seg.polygonId}")
          println(ids)
        }
        setNextAndPrev(inner_segments)
        val (inner_closed, inner_open) = inner_segments.partition(_.isClose)
        // closing pointer in polygons fully contained...
        inner_closed.map{ seg =>
          seg.last.next = seg.first
          seg.first.prev = seg.last 
        }

        //println("inner_segments")
        inner_segments.map{ inner =>
          s"${inner.wkt}\t${inner.polygonId}"
        }.foreach{println}

        //println("Merge")
        merge(inner.filter(_.data.crossingInfo != "None"),  borders)
        inner.map{_.getPolygon.toText}.foreach(println)
      }

      val hedges = groupByNext((inner).toSet, List.empty[(Half_edge, String)])
      hedges.toIterator
      //(inner ++ borders).toIterator
      //it
    }
    //save("/tmp/edgesCross.wkt"){r}
    r
  }

  // Sequential implementation of dcel...
  def sequential(hedges_prime: List[Half_edge], partitionId: Int = -1): Unit = {
    // Group half-edges by the destination vertex (v2)...
    val hedges = hedges_prime ++ hedges_prime.filter(_.twin.isNewTwin).map(_.twin)
    val incidents = hedges.groupBy(_.v2).values.toList

    // At each vertex, get their incident half-edges...
    val h_prime = incidents.map{ hList =>
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
    }.flatten
  }

  def merge(crossing0: List[Half_edge], borders0: List[Half_edge]): Unit = {
    val crossing = crossing0 ++ crossing0.filter(_.twin.isNewTwin).map(_.twin)
    val vertices = borders0.map(_.v2)
    val borders =  borders0 ++ borders0.filter(_.twin.isNewTwin).map(_.twin)

    val hedges = (borders ++ crossing).filter(h => vertices.contains(h.v2))
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

        //println(hedges.head.data.polygonId)
        //println(hedges.map{_.data.edgeId}.mkString(" "))

        val ins = getLineSegments(hedges.tail, List(hedges.head),
          List.empty[List[Half_edge]])

        val segs = ins.map{Segment}
        if(segs.head.startId == 0){
          val lastEdgeId = segs.last.last.data.edgeId
          val nEdgesPoly = segs.last.last.data.nedges
          if(lastEdgeId == nEdgesPoly){
            val head = segs.head
            val last = segs.last
            val new_head = Segment(last.hedges ++ head.hedges)
            new_head +: segs.slice(1, segs.length - 1)
          } else{
            segs
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

    crossing.filter{ edge =>
      // Remove edges that touch the same border twice...
      // They will be repeated in the borders edges...
      val crossing_info = getCrossingInfo(edge).split("\\|")
      val borders = crossing_info.map(_.split(":")(0)).toList
      val borders_dist = borders.distinct
      borders.size == borders_dist.size
    }.map{ edge =>
      val crossing_info = getCrossingInfo(edge).split("\\|")
      val nsplits = crossing_info.size

      nsplits match {
        // Single case, the edge has only one intersection.
        case 1 => {
          val arr = crossing_info.head.split(":")
          val border = arr(0)
          val xy = arr(1).split(" ") 
          val coord = new Coordinate(xy(0).toDouble, xy(1).toDouble)
          val split = splitEdge(edge, border, coord)
          split.setUserData(edge.getUserData)
          split
        }
        case 2 => {
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
          val split = if(c1 != c2){
            val split = geofactory.createLineString(Array(c1, c2))
            split
          } else {
            // If the two coords are the same is because the edge touch just a corner.
            // We select a coord and solve it as an edge with a single intersection point.
            val arr = crossing_info.head.split(":")
            val border = arr(0)
            val xy = arr(1).split(" ")
            val coord = new Coordinate(xy(0).toDouble, xy(1).toDouble)
            val split = splitEdge(edge, border, c1)
            split
          }

          split.setUserData(edge.getUserData)
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
        if (start.y >= coord.y) Array(coord, end) else Array(start, coord)
      }
      case "S" => {
        if (start.y <= coord.y) Array(coord, end) else Array(start, coord)
      }
      case "W" => {
        if (start.x >= coord.x) Array(coord, end) else Array(start, coord)
      }
      case "E" => {
        if (start.x <= coord.x) Array(coord, end) else Array(start, coord)
      }
    }

    geofactory.createLineString(coords)
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

  // Return list of coordinate intersection on a particular border (S,W,N or E)...
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
      case "W" => intersections.sortBy(_.y)
      case "N" => intersections.sortBy(_.x)(Ordering[Double].reverse)
      case "E" => intersections.sortBy(_.y)(Ordering[Double].reverse)
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
