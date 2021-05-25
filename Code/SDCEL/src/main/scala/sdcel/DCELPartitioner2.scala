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
import Utils._

object DCELPartitioner2 {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def read(input: String)
    (implicit spark: SparkSession, geofactory: GeometryFactory): SpatialRDD[LineString] = {

    val polys = spark.read.textFile(input).rdd.persist
    val nPolys = polys.count
    logger.info(s"# of Polys:\t$nPolys")
    val edgesRaw = polys.mapPartitionsWithIndex{ case(index, lines) =>
      val reader = new WKTReader(geofactory)
      lines.flatMap{ line =>
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
    logger.info("Starting session...")
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
    logger.info(command)
    logger.info(s"Scale: ${settings.scale}")
    val model = new PrecisionModel(settings.scale)
    implicit val geofactory = new GeometryFactory(model)
    logger.info("Starting session... Done!")

    // Reading data...
    logger.info("Reading data...")
    val edgesRDDA = read(params.input1())
    val nEdgesRDDA = edgesRDDA.getRawSpatialRDD.count()
    logger.info(s"# of Edges:\t$nEdgesRDDA")
    val edgesRDDB = read(params.input2())
    val nEdgesRDDB = edgesRDDB.getRawSpatialRDD.count()
    logger.info(s"# of Edges:\t$nEdgesRDDB")
    val edgesRDD = edgesRDDA.getRawSpatialRDD.rdd  union edgesRDDB.getRawSpatialRDD.rdd
    val nEdgesRDD = nEdgesRDDA + nEdgesRDDB
    
    val boundary = edgesRDDA.boundary
    boundary.expandToInclude(edgesRDDB.boundary)
    logger.info("Reading data... Done!")

    // Partitioning data...
    logger.info("Partitioning data...")
    val (quadtree, edgesA, edgesB) = if(params.bycapacity()){
      logger.info(s"Partition by capacity (${params.maxentries()})")
      val definition = new QuadRectangle(boundary)
      val maxentries = params.maxentries()
      val maxlevel   = params.maxlevel()
      val fraction   = params.fraction()
      val quadtree = new StandardQuadTree[LineString](definition, 0, maxentries, maxlevel)
      val samples = edgesRDD.sample(false, fraction, 42).collect()
      logger.info(s"Sample size: ${samples.size}")
      samples.foreach{ edge =>
        quadtree.insert(new QuadRectangle(edge.getEnvelopeInternal), edge)
      }
      quadtree.assignPartitionIds
      quadtree.assignPartitionLineage
      val partitioner = new QuadTreePartitioner(quadtree)
      edgesRDDA.spatialPartitioning(partitioner)
      val edgesA = edgesRDDA.spatialPartitionedRDD.rdd.persist()
      edgesRDDB.spatialPartitioning(partitioner)
      val edgesB = edgesRDDB.spatialPartitionedRDD.rdd.persist()
      (quadtree, edgesA, edgesB)
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
      val partitioner = new QuadTreePartitioner(quadtree)
      edgesRDDA.spatialPartitioning(partitioner)
      val edgesA = edgesRDDA.spatialPartitionedRDD.rdd.persist()
      edgesRDDB.spatialPartitioning(partitioner)
      val edgesB = edgesRDDB.spatialPartitionedRDD.rdd.persist()
      (quadtree, edgesA, edgesB)
    }
    logger.info("Partitioning data... Done")
    logger.info(s"Number of partitions: ${quadtree.getLeafZones.size}")

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
    
    // Saving to HDFS or Local...
    if(!params.local()){
      saveToHDFS(edgesA, params.apath())
      saveToHDFS(edgesB, params.bpath())
    } else {
      saveToLocal(edgesA, params.apath())
      saveToLocal(edgesB, params.bpath())
    }

    spark.close
  }

  def saveToLocal(edges: RDD[LineString], name: String): Unit = {
    save{name}{
      edges.mapPartitionsWithIndex{ (index, edgesIt) =>
        edgesIt.map{ edge =>
          val wkt = edge.toText
          val data = edge.getUserData

          s"$wkt\t$index\t$data\n"
        }
      }.collect
    }
  }

  def saveToHDFS(edges: RDD[LineString], name: String)
    (implicit spark: SparkSession): Unit = {
    import spark.implicits._
    edges.mapPartitionsWithIndex{ (index, edgesIt) =>
      edgesIt.map{ edge =>
        val wkt = edge.toText
        val data = edge.getUserData

        s"$wkt\t$index\t$data"
      }
    }.toDF.write
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .text(name)
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
