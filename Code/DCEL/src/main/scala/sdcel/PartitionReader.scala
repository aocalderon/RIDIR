package edu.ucr.dblab.sdcel

import scala.collection.JavaConverters._
import scala.io.Source
import sys.process._
import com.vividsolutions.jts.geom.{Coordinate, Envelope}
import com.vividsolutions.jts.geom.{LineString, Polygon, LinearRing}
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.io.WKTReader
import com.vividsolutions.jts.algorithm.CGAlgorithms
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import edu.ucr.dblab.sdcel.quadtree._
import edu.ucr.dblab.sdcel.geometries._

object PartitionReader {

  def readEdges(spark: SparkSession, params: Params,
    input: String)
    (implicit geofactory: GeometryFactory):
      (RDD[LineString], StandardQuadTree[Nothing], Map[Int, Cell]) = {
    // Reading data...
    val partitions = if(params.local()){
      params.partitions()
    } else {
      val parts = (s"hdfs dfs -ls ${input}/part*" #| "tail -n 1" !!)
      .split("/").reverse.head.split("-")(1).toInt
      parts + 1
    }
    val home = System.getProperty("user.home")
    val path = s"${home}/RIDIR/Code/Scripts/gadm"
    
    val edgesRDD = spark.read.textFile(input).rdd
      .mapPartitionsWithIndex{ case(index, lines) =>
        val reader = new WKTReader(geofactory)
        lines.map{ line =>
          val arr = line.split("\t")
          val wkt = arr(0)
          val partitionId = arr(1).toInt
          val polygonId = arr(2).toInt
          val ringId = arr(3).toInt
          val edgeId = arr(4).toInt
          val isHole = arr(5).toBoolean
          val edge = reader.read(wkt).asInstanceOf[LineString]
          val data = EdgeData(polygonId, ringId, edgeId, isHole)
          edge.setUserData(data)
          (partitionId, edge)
        }.toIterator
      }.partitionBy(new SimplePartitioner(partitions))
      .map(_._2).persist()
    val nEdgesRDD = edgesRDD.count()

    // Reading quadtree...
    val quadtreeBuff = Source.fromFile(s"${path}/quadtree.wkt")
    val quads = quadtreeBuff.getLines.map{ line =>
      val arr = line.split("\t")
      val reader = new WKTReader(geofactory)
      val envelope = reader.read(arr(0)).getEnvelopeInternal
      val minX = envelope.getMinX
      val maxX = envelope.getMaxX
      val minY = envelope.getMinY
      val maxY = envelope.getMaxY
      val lineage = arr(2)
      
      (lineage, (minX, maxX, minY, maxY))
    }.toList
    val lineages = quads.map(_._1)
    val stats = quads.map(_._2)
    val boundary = new Envelope(
      stats.map(_._1).min,
      stats.map(_._2).max,
      stats.map(_._3).min,
      stats.map(_._4).max
    )
    val quadtree = Quadtree.create(boundary, lineages)
    quadtreeBuff.close

    // Getting cells...
    val cells = quadtree.getLeafZones.asScala.map{ leaf =>
      val id = leaf.partitionId.toInt
      val lineage = leaf.lineage
      val mbr = envelope2ring(roundEnvelope(leaf.getEnvelope))

      val cell = Cell(id, lineage, mbr)
      (id -> cell)
    }.toMap

    (edgesRDD, quadtree, cells)
  }

  def getLineStrings(polygon: Polygon, polygon_id: Long)
    (implicit geofactory: GeometryFactory): List[LineString] = {

    getRings(polygon).zipWithIndex
      .flatMap{ case(ring, ring_id) =>
        ring.zip(ring.tail).zipWithIndex.map{ case(pair, order) =>
          val coord1 = pair._1
          val coord2 = pair._2
          val coords = Array(coord1, coord2)
          val isHole = ring_id > 0

          val line = geofactory.createLineString(coords)
          line.setUserData(s"$polygon_id\t$ring_id\t$order\t${isHole}")

          line
        }
    }
  }

  private def getRings(polygon: Polygon): List[Array[Coordinate]] = {
    val ecoords = polygon.getExteriorRing.getCoordinateSequence.toCoordinateArray()
    val outerRing = if(!CGAlgorithms.isCCW(ecoords)) { ecoords.reverse } else { ecoords }
    
    val nInteriorRings = polygon.getNumInteriorRing
    val innerRings = (0 until nInteriorRings).map{ i => 
      val icoords = polygon.getInteriorRingN(i).getCoordinateSequence.toCoordinateArray()
      if(CGAlgorithms.isCCW(icoords)) { icoords.reverse } else { icoords }
    }.toList

    outerRing +: innerRings
  }

  def envelope2polygon(e: Envelope)(implicit geofactory: GeometryFactory): Polygon = {
    val W = e.getMinX()
    val S = e.getMinY()
    val E = e.getMaxX()
    val N = e.getMaxY()
    val WS = new Coordinate(W, S)
    val ES = new Coordinate(E, S)
    val EN = new Coordinate(E, N)
    val WN = new Coordinate(W, N)
    geofactory.createPolygon(Array(WS,ES,EN,WN,WS))
  }

  def envelope2ring(e: Envelope)(implicit geofactory: GeometryFactory): LinearRing = {
    val W = e.getMinX()
    val S = e.getMinY()
    val E = e.getMaxX()
    val N = e.getMaxY()
    val WS = new Coordinate(W, S)
    val ES = new Coordinate(E, S)
    val EN = new Coordinate(E, N)
    val WN = new Coordinate(W, N)
    geofactory.createLinearRing(Array(WS,ES,EN,WN,WS))
  }

  def roundEnvelope(envelope: Envelope)(implicit geofactory: GeometryFactory): Envelope = {
    val scale = geofactory.getPrecisionModel.getScale
    val e = round(envelope.getMinX, scale)
    val w = round(envelope.getMaxX, scale)
    val s = round(envelope.getMinY, scale)
    val n = round(envelope.getMaxY, scale)
    new Envelope(e, w, s, n)
  }
  private def round(number: Double, scale: Double): Double =
    Math.round(number * scale) / scale

}
