package edu.ucr.dblab.sdcel.reader

import com.vividsolutions.jts.algorithm.CGAlgorithms
import com.vividsolutions.jts.geom._
import com.vividsolutions.jts.io.WKTReader
import edu.ucr.dblab.sdcel.PartitionReader.envelope2ring
import edu.ucr.dblab.sdcel.Utils.{Settings, log}
import edu.ucr.dblab.sdcel.geometries.Cell
import edu.ucr.dblab.sdcel.quadtree.StandardQuadTree
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters.asScalaBufferConverter

object PR_Utils {
  def read(input: String)(implicit spark: SparkSession, G: GeometryFactory, S: Settings): RDD[LineString] = {

    val polys = spark.read.textFile(input).rdd.cache()
    debug{
      val nPolys = polys.count
      log(s"INFO|npolys=$nPolys")
    }
    val edgesRaw = polys.mapPartitions{ lines =>
      val reader = new WKTReader(G)
      lines.flatMap{ line =>
        val arr = line.split("\t")
        val wkt = arr(0)
        val id  = arr(1).toLong
        val geom = reader.read(wkt)
        (0 until geom.getNumGeometries).map{ i =>
          (geom.getGeometryN(i).asInstanceOf[Polygon], id)
        }.flatMap{ case(polygon, id) =>
          getLineStrings(polygon, id)
        }.toIterator
      }
    }.cache()

    edgesRaw
  }

  private def getLineStrings(polygon: Polygon, polygon_id: Long)(implicit geofactory: GeometryFactory): List[LineString] = {
    getRings(polygon)
      .zipWithIndex
      .flatMap{ case(ring, ring_id) =>
        ring.zip(ring.tail).zipWithIndex.map{ case(pair, order) =>
          val coord1 = pair._1
          val coord2 = pair._2
          val coords = Array(coord1, coord2)
          val isHole = ring_id > 0
          val n = polygon.getNumPoints - 2

          val line = geofactory.createLineString(coords)
          // Save info from the edge...
          line.setUserData(s"$polygon_id\t$ring_id\t$order\t$isHole\t$n")

          line
        }
      }
  }

  private def getRings(polygon: Polygon): List[Array[Coordinate]] = {
    val exterior_coordinates = polygon.getExteriorRing.getCoordinateSequence.toCoordinateArray
    val outerRing = if(!CGAlgorithms.isCCW(exterior_coordinates)) { exterior_coordinates.reverse } else { exterior_coordinates }

    val nInteriorRings = polygon.getNumInteriorRing
    val innerRings = (0 until nInteriorRings).map{ i =>
      val interior_coordinates = polygon.getInteriorRingN(i).getCoordinateSequence.toCoordinateArray
      if(CGAlgorithms.isCCW(interior_coordinates)) { interior_coordinates.reverse } else { interior_coordinates }
    }.toList

    outerRing +: innerRings
  }

  def getStudyArea(edges: RDD[LineString]): Envelope = {
    edges.map(_.getEnvelopeInternal).reduce { (a, b) =>
      a.expandToInclude(b)
      a
    }
  }

  def getCells(quadtree: StandardQuadTree[LineString])(implicit G: GeometryFactory): Map[Int, Cell] = {
    quadtree.getLeafZones.asScala.map { leaf =>
      val id = leaf.partitionId.toInt
      val lineage = leaf.lineage
      val envelope = leaf.getEnvelope
      val mbr = envelope2ring(envelope)

      val cell = Cell(id, lineage, mbr)
      id -> cell
    }.toMap
  }


  def debug[R](block: => R)(implicit S: Settings): Unit = { if(S.debug) block }

}
