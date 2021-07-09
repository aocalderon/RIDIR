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

    val partitionId = 3

    val r = edgesRDD.mapPartitionsWithIndex{ (pid, it) =>
      val cell = cells(pid)
      val (containedIt, crossingIt) = classifyEdges(it)

      val crossing = crossingIt.toList

      val SS = crossing.filter(e => getCrossingInfo(e).contains("S:"))
      val WW = crossing.filter(e => getCrossingInfo(e).contains("W:"))
      val NN = crossing.filter(e => getCrossingInfo(e).contains("N:"))
      val EE = crossing.filter(e => getCrossingInfo(e).contains("E:"))

      val S = getIntersectionsOnBorder(crossing, "S")
      val W = getIntersectionsOnBorder(crossing, "W")
      val N = getIntersectionsOnBorder(crossing, "N")
      val E = getIntersectionsOnBorder(crossing, "E")

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
        println("Data")
        SS.foreach{println}
        println
        WW.foreach{println}
        println
        NN.foreach{println}
        println
        EE.foreach{println}
        println
      }

      it
    }.collect
    
  }

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
      }
    intersections.distinct
  }

  def classifyEdges(edges: Iterator[LineString]):
      (Iterator[LineString], Iterator[LineString]) = {

    edges.partition(edge => getCrossingInfo(edge) == "None")
  }

  private def getCrossingInfo(edge: LineString): String = {
    val data = edge.getUserData.asInstanceOf[EdgeData]
    data.crossingInfo
  }
}
