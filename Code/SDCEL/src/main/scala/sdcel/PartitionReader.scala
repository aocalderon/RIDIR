package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.algorithm.CGAlgorithms
import com.vividsolutions.jts.geom._
import com.vividsolutions.jts.io.WKTReader
import edu.ucr.dblab.sdcel.Utils._
import edu.ucr.dblab.sdcel.geometries._
import edu.ucr.dblab.sdcel.quadtree._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._
import scala.io.Source

object PartitionReader {
  def readQuadtree[T](qpath: String, epath: String)
    (implicit geofactory: GeometryFactory, settings: Settings):
      (StandardQuadTree[T], Map[Int, Cell]) = {

    // Reading quadtree...
    val quadtreeBuff = Source.fromFile(qpath)
    // lineages are the first column in the file...
    val lineages = quadtreeBuff.getLines.map(_.split("\t").head).toList
    quadtreeBuff.close

    // Reading boundary...
    val boundaryBuff = Source.fromFile(epath)
    val boundaryWkt = boundaryBuff.getLines.next // boundary is the only line in the file...
    val reader = new WKTReader(geofactory)
    val boundary = reader.read(boundaryWkt).getEnvelopeInternal
    boundaryBuff.close
    val quadtree = Quadtree.create[T](boundary, lineages)

    // Getting cells...
    val cells = quadtree.getLeafZones.asScala.map{ leaf =>
      val id = leaf.partitionId.toInt
      val lineage = leaf.lineage
      val envelope = roundEnvelope(leaf.getEnvelope)
      val mbr = envelope2ring(envelope)

      val cell = Cell(id, lineage, mbr)
      (id -> cell)
    }.toMap

    if(settings.debug){
      log(s"INFO|npartitions=${cells.size}")
      save{s"/tmp/edgesQ.wkt"}{
        cells.values.map{ cell =>
          val wkt = cell.wkt
          s"$wkt\n"
        }.toList
      }
    }    

    (quadtree, cells)
  }

  def readEdges[T](input: String, label: String)
    (implicit geofactory: GeometryFactory, spark: SparkSession, settings: Settings,
     cells: Map[Int, Cell]): RDD[LineString] = {
    
    // Reading data...
    val partitions = cells.size
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
          val nedges = try { arr(6).toInt }
          catch { case e: java.lang.ArrayIndexOutOfBoundsException => -1 }
          val cross  = try { arr(7) }
          catch { case e: java.lang.ArrayIndexOutOfBoundsException => "" } 
          val edge = reader.read(wkt).asInstanceOf[LineString]
          val data = EdgeData(polygonId, ringId, edgeId, isHole, label, cross, nedges)
          edge.setUserData(data)
          (partitionId, edge)
        }.toIterator
      }
      .partitionBy(new SimplePartitioner(partitions))
      .map(_._2)
      .persist(settings.persistance)

    edgesRDD
  }

  def readEdges_byPartition[T](input: String, label: String)
    (implicit geofactory: GeometryFactory, spark: SparkSession, settings: Settings):
      RDD[LineString] = {
    
    // Reading data...
    val partitions = 1
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
          val nedges = try { arr(6).toInt }
          catch { case e: java.lang.ArrayIndexOutOfBoundsException => -1 }
          val cross  = try { arr(7) }
          catch { case e: java.lang.ArrayIndexOutOfBoundsException => "" } 
          val edge = reader.read(wkt).asInstanceOf[LineString]
          val data = EdgeData(polygonId, ringId, edgeId, isHole, label, cross, nedges)
          edge.setUserData(data)
          (0, edge)
        }.toIterator
      }
      .partitionBy(new SimplePartitioner(partitions))
      .map(_._2)
      .persist(settings.persistance)

    edgesRDD
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

  def envelope2polygon(e: Envelope)
    (implicit geofactory: GeometryFactory): Polygon = {

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

  def envelope2ring(e: Envelope)
    (implicit geofactory: GeometryFactory): LinearRing = {

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

  def roundEnvelope(envelope: Envelope)
    (implicit geofactory: GeometryFactory): Envelope = {

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
