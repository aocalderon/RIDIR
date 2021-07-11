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

import DCELMerger2.{setTwins}
import Utils._

object LocalDCEL {
  def createLocalDCELs(edgesRDD: RDD[LineString], cells: Map[Int, Cell])
    (implicit geofactory: GeometryFactory, logger: Logger, spark: SparkSession,
    settings: Settings)
      : Unit = {

    val partitionId = 9

    val r = edgesRDD.mapPartitionsWithIndex{ (pid, it) =>

      val (containedIt, crossingIt) = classifyEdges(it)

      val crossing = crossingIt.toList
      val S = getIntersectionsOnBorder(crossing, "S")
      val W = getIntersectionsOnBorder(crossing, "W")
      val N = getIntersectionsOnBorder(crossing, "N")
      val E = getIntersectionsOnBorder(crossing, "E")

      val cell = cells(pid)
      val bordersS = splitBorder(cell.getSouthBorder, S)
      val bordersW = splitBorder(cell.getWestBorder, W)
      val bordersN = splitBorder(cell.getNorthBorder, N)
      val bordersE = splitBorder(cell.getEastBorder, E)
      val borders = (bordersS ++ bordersW ++ bordersN ++ bordersE)

      val outs = (borders.map{Half_edge} ++ getCrossingHalf_edges(crossing)).distinct
      val ins = containedIt.map{Half_edge}.toList

      // pair each half_edge with its twin...
      setTwins(outs)
      setTwins(ins)
      setNextAndPrev(ins)

      // run sequential routine to create dcel for outs...
      sequential(outs)

      if(pid == partitionId){
        println(s"PID $partitionId:")
        S.foreach{println}
        println
        W.foreach{println}
        println
        N.foreach{println}
        println
        E.foreach{println}
        println

        println("outs")
        outs.map(_.wkt).foreach{println}

        println("output")
        outs.map{ o => s"I am $o and my next is ${o.next} and my prev is ${o.prev}"}
          .foreach(println)

        println("polygon")
        
        outs.filter(_.data.polygonId != -1)  // Remove outer face
          .groupBy{_.data.polygonId}.values.map(_.head) // Pick only one half-edge per polygon...
          .map(_.getPolygon.toText).foreach{println}
      }

      (outs ++ outs.filter(_.twin.isNewTwin).map(_.twin) ++
        ins ++ ins.filter(_.twin.isNewTwin).map(_.twin))
        .map(b => s"${b.wkt}\t${b.data}\n").toIterator

      //it
    }.collect
    save("/tmp/edgesCross.wkt"){r}
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

  // Set the next and prev pointer for the inners edges based on its edgeId...
  def setNextAndPrev(ins: List[Half_edge]): List[Half_edge] = {
    ins.groupBy{in => (in.data.polygonId, in.data.ringId)}
      .flatMap{ case(pid, hedges_prime) =>
        val hedges = hedges_prime.sortBy(_.data.edgeId)
        hedges.zip(hedges.tail).map{ case(h1, h2) =>
          h1.next = h2
          h2.prev = h1
        }
        hedges
      }.toList
  }

  // Split crossing edges according to the cell border they intersect...
  // Return them as half-edges to keep information about the original edge...
  def getCrossingHalf_edges(crossing: List[LineString])
    (implicit geofactory: GeometryFactory): List[Half_edge] = {

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
          val hedge = Half_edge(split)
          hedge.original_coords = edge.getCoordinates
          hedge
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
            split.setUserData(edge.getUserData)
            split
          } else {
            // If the two coords are the same is because the edge touch just a corner.
            // We select a coord and solve it as an edge with a single intersection point.
            val arr = crossing_info.head.split(":")
            val border = arr(0)
            val xy = arr(1).split(" ")
            val coord = new Coordinate(xy(0).toDouble, xy(1).toDouble)
            val split = splitEdge(edge, border, c1)
            split.setUserData(edge.getUserData)
            split
          }

          val hedge = Half_edge(split)
          hedge.original_coords = edge.getCoordinates
          hedge
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
}
