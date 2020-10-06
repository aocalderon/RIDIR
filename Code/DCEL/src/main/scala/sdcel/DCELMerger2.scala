package edu.ucr.dblab.sdcel

import scala.collection.JavaConverters._
import scala.io.Source
import sys.process._
import com.vividsolutions.jts.geom.{Coordinate, Envelope}
import com.vividsolutions.jts.geom.{LineString, Polygon, LinearRing}
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.io.WKTReader
import com.vividsolutions.jts.algorithm.CGAlgorithms
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.slf4j.{Logger, LoggerFactory}
import edu.ucr.dblab.sdcel.quadtree._
import edu.ucr.dblab.sdcel.geometries._

object DCELMerger2 {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  case class Cell(id: Int, lineage: String, mbr: LinearRing)

  def main(args: Array[String]) = {
    // Starting session...
    logger.info("Starting session...")
    implicit val spark = SparkSession.builder()
        .config("spark.serializer",classOf[KryoSerializer].getName)
        .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
        .getOrCreate()
    import spark.implicits._
    implicit val model = new PrecisionModel(1000)
    implicit val geofactory = new GeometryFactory(model)
    val precision = 1.0 / model.getScale
    val params = new Params(args)
    val input = params.input1()
    val local = params.local()
    val home = System.getProperty("user.home")
    val path = s"${home}/RIDIR/Code/Scripts/gadm"
    logger.info(s"Using $precision precision.")
    logger.info("Starting session... Done!")

    // Reading data...
    val partitions = if(local){
      params.partitions()
    } else {
      val parts = (s"hdfs dfs -ls ${input}/part*" #| "tail -n 1" !!)
      .split("/").reverse.head.split("-")(1).toInt
      parts + 1
    }
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
    logger.info("Reading file done!")

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
    save{"/tmp/edgesCells.wkt"}{
      cells.values.map{ cell =>
        val wkt = envelope2polygon(cell.mbr.getEnvelopeInternal).toText
        val id = cell.id
        val lineage = cell.lineage
        s"$wkt\t$id\t$lineage\n"
      }.toList
    }

    // Getting LDCELs...
    val dcels = edgesRDD.mapPartitionsWithIndex{ case (index, edgesIt) =>
      val cell = cells(index).mbr
      val envelope = cell.getEnvelopeInternal
      
      val (outerEdges, innerEdges) = edgesIt.partition{ edge =>
        cell.intersects(edge)
      }

      println(index)
      val outer = SweepLine2.getHedgesTouchingCell(outerEdges.toVector, cell)
      val inner = SweepLine2.getHedgesInsideCell(innerEdges.toVector, index)
      val h = SweepLine2.merge(outer, inner)
      //if(index == 14)
        //h.map(_.getNextsAsWKT).foreach{println}
     

      val r = (index, outer, inner, h)
      Iterator(r)
    }.cache
    val n = dcels.count()
    logger.info("Getting LDCELs done!")

    save{"/tmp/edgesHout.wkt"}{
      dcels.mapPartitionsWithIndex{ (index, dcelsIt) =>
        val dcel = dcelsIt.next
        dcel._2.map{ h =>
          val wkt = makeWKT(h) 
          val pid = h.head.data.polygonId
          val eid = h.head.data.edgeId
          
          s"$wkt\t$pid:$eid\t$index\n"
        }.toIterator
      }.collect
    }
    save{"/tmp/edgesHin.wkt"}{
      dcels.mapPartitionsWithIndex{ (index, dcelsIt) =>
        val dcel = dcelsIt.next
        dcel._3.map{ h =>
          val wkt = makeWKT(h) 
          val pid = h.head.data.polygonId
          val eid = h.head.data.edgeId
          
          s"$wkt\t$pid:$eid\t$index\n"
        }.toIterator
      }.collect
    }
    save{"/tmp/edgesV.wkt"}{
      dcels.mapPartitionsWithIndex{ (index, dcelsIt) =>
        val dcel = dcelsIt.next
        dcel._4.zipWithIndex.map{ case(v, id) =>
          val wkt = v.getNextsAsWKT //geofactory.createPoint(v).toText
          s"$wkt\t$id\t$index\n"
        }.toIterator
      }.collect
    }
    
    spark.close
  }

  def makeWKT(hedges: List[Half_edge])(implicit geofactory: GeometryFactory): String = {
    val coords = hedges.map{_.v1} :+ hedges.last.v2
    geofactory.createLineString(coords.toArray).toText
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

  def roundEnvelope(envelope: Envelope)(implicit model: PrecisionModel): Envelope = {
    val scale = model.getScale
    val e = round(envelope.getMinX, scale)
    val w = round(envelope.getMaxX, scale)
    val s = round(envelope.getMinY, scale)
    val n = round(envelope.getMaxY, scale)
    new Envelope(e, w, s, n)
  }
  private def round(number: Double, scale: Double): Double =
    Math.round(number * scale) / scale

  def save(filename: String)(content: Seq[String]): Unit = {
    val start = clocktime
    val f = new java.io.PrintWriter(filename)
    f.write(content.mkString(""))
    f.close
    val end = clocktime
    val time = "%.2f".format((end - start) / 1000.0)
    logger.info(s"Saved ${filename} in ${time}s [${content.size} records].")
  }
  private def clocktime = System.currentTimeMillis()
}
