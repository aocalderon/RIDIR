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

import Utils._

object LocalDCEL {
  def createLocalDCELs(edgesRDD: RDD[LineString], cells: Map[Int, Cell])
    (implicit geofactory: GeometryFactory, logger: Logger, spark: SparkSession,
    settings: Settings)
      : Unit = {

    val partitionId = 15

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

      // Need to test all the partitions...
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

        println("Splits")
        getCrossingHalf_edges(crossing)
      }

        getCrossingHalf_edges(crossing)
        .map(b => s"${b.wkt}\n").toIterator

      //it
    }.collect
    save("/tmp/edgesCross.wkt"){r}
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
            // solve special case for edge in corner...
            // select a coord and solve as it was a edge with a single intersection point
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
