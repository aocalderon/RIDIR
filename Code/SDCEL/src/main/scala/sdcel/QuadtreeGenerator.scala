package edu.ucr.dblab.sdcel

import scala.collection.JavaConverters._
import com.vividsolutions.jts.geom.{Coordinate, Envelope}
import com.vividsolutions.jts.geom.{LineString, Polygon}
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.io.WKTReader
import com.vividsolutions.jts.algorithm.CGAlgorithms
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.slf4j.{Logger, LoggerFactory}
import edu.ucr.dblab.sdcel.quadtree._
import edu.ucr.dblab.sdcel.geometries.{Cell, LEdge}

import Utils._

object QuadtreeGenerator {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def read(input: String)
    (implicit spark: SparkSession, geofactory: GeometryFactory, settings: Settings):
      SpatialRDD[LineString] = {

    val polys = spark.read.textFile(input).rdd.persist
    if(settings.debug){
      val nPolys = polys.count
      logger.info(s"TIME|npolys$nPolys")
    }
    val edgesRaw = polys.mapPartitionsWithIndex{ case(index, lines) =>
      val reader = new WKTReader(geofactory)
      lines.flatMap{ line0 =>
        val line = line0.split("\t")(0)
        val geom = reader.read(line.replaceAll("\"", ""))
          (0 until geom.getNumGeometries).map{ i =>
            geom.getGeometryN(i).asInstanceOf[Polygon]
          }
      }.toIterator
    }.zipWithIndex.flatMap{ case(polygon, id) =>
        getLineStrings(polygon, id)
    }.persist
    val edgesRDD = new SpatialRDD[LineString]()
    edgesRDD.setRawSpatialRDD(edgesRaw)
    edgesRDD.analyze()

    edgesRDD
  }

  def main(args: Array[String]) = {
    // Starting session...
    implicit val spark = SparkSession.builder()
        .config("spark.serializer",classOf[KryoSerializer].getName)
        .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
        .getOrCreate()
    import spark.implicits._
    val params = new Params(args)
    implicit val settings = Settings(
      tolerance = params.tolerance(),
      debug = params.debug(),
      local = params.local(),
      appId = spark.sparkContext.applicationId
    )
    val command = System.getProperty("sun.java.command")
    log(s"COMMAND|$command")
    log(s"INFO|scale=${settings.scale}")
    val model = new PrecisionModel(settings.scale)
    implicit val geofactory = new GeometryFactory(model)
    log("TIME|Start")

    // Reading data...
    val edgesRDDA = read(params.input1())
    val nEdgesRDDA = edgesRDDA.getRawSpatialRDD.count()
    log(s"INFO|edgesA=$nEdgesRDDA")
    val edgesRDDB = read(params.input2())
    val nEdgesRDDB = edgesRDDB.getRawSpatialRDD.count()
    log(s"INFO|edgesB=$nEdgesRDDB")
    val edgesRDD = edgesRDDA.getRawSpatialRDD.rdd  union edgesRDDB.getRawSpatialRDD.rdd
    val nEdgesRDD = nEdgesRDDA + nEdgesRDDB
    val boundary = edgesRDDA.boundary
    boundary.expandToInclude(edgesRDDB.boundary)
    log("TIME|Read")

    // Creating quadtree...
    val quadtree = if(params.bycapacity()){
      log(s"INFO|capacity=${params.maxentries()}")
      val definition = new QuadRectangle(boundary)
      val maxentries = params.maxentries()
      val maxlevel   = params.maxlevel()
      val fraction   = params.fraction()
      val quadtree = new StandardQuadTree[LineString](definition, 0, maxentries, maxlevel)
      val samples = edgesRDD.sample(false, fraction, 42).collect()
      log(s"INFO|sample=${samples.size}")
      log("TIME|Sample")
      samples.foreach{ edge =>
        quadtree.insert(new QuadRectangle(edge.getEnvelopeInternal), edge)
      }
      quadtree.assignPartitionIds
      quadtree.assignPartitionLineage
      log("TIME|Quadtree")
      quadtree
    } else {
      logger.info(s"Partition by number (${params.partitions()})")
      val fc = new FractionCalculator()
      val fraction = fc.getFraction(params.partitions(), nEdgesRDD)
      logger.info(s"Fraction: ${fraction}")
      val samples = edgesRDD.sample(false, fraction, 42)
        .map(_.getEnvelopeInternal).collect().toList.asJava
      val partitioning = new QuadtreePartitioning(samples,
        boundary, params.partitions())
      val quadtree = partitioning.getPartitionTree()
      quadtree.assignPartitionLineage()
      quadtree
    }
    log(s"INFO|partitions=${quadtree.getLeafZones.size}")
    log("TIME|Partition")

    // Saving the boundary...
    save{params.epath()}{
      val wkt = envelope2polygon(boundary).toText
      List(s"$wkt\n")
    }
    // Saving the quadtree
    save{params.qpath()}{
      quadtree.getLeafZones.asScala.map{ leaf =>
        val id = leaf.partitionId.toInt
        val lineage = leaf.lineage
        val wkt = envelope2polygon(roundEnvelope(leaf.getEnvelope)).toText

        s"$lineage\t$id\t$wkt\n"
      }
    }
    
    spark.close
    log("TIME|Close")
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
          val n = polygon.getNumPoints - 2

          val line = geofactory.createLineString(coords)
          // Save info from the edge...
          line.setUserData(s"$polygon_id\t$ring_id\t$order\t${isHole}\t${n}")

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
    val minX = e.getMinX()
    val minY = e.getMinY()
    val maxX = e.getMaxX()
    val maxY = e.getMaxY()
    val p1 = new Coordinate(minX, minY)
    val p2 = new Coordinate(maxX, minY)
    val p3 = new Coordinate(maxX, maxY)
    val p4 = new Coordinate(minX, maxY)
    geofactory.createPolygon(Array(p1,p2,p3,p4,p1))
  }

  private def roundEnvelope(envelope: Envelope)
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
