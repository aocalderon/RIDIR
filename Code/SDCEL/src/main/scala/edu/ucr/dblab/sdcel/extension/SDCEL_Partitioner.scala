package edu.ucr.dblab.sdcel.extension

import com.vividsolutions.jts.geom.{Envelope, GeometryFactory, LineString, PrecisionModel}
import edu.ucr.dblab.sdcel.Params
import edu.ucr.dblab.sdcel.Utils.{Settings, log, save, timer}
import edu.ucr.dblab.sdcel.kdtree.KDBTree
import edu.ucr.dblab.sdcel.quadtree.{QuadRectangle, StandardQuadTree}
import edu.ucr.dblab.sdcel.reader.PR_Utils.{getStudyArea, read}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters.{asScalaBufferConverter, mapAsScalaMapConverter}
import scala.util.Random

object SDCEL_Partitioner {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")
  def main(args: Array[String]): Unit = {
    // Starting session...
    implicit val params = new Params(args)
    implicit val spark: SparkSession = SparkSession.builder()
      .master(params.master())
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()
    implicit val S: Settings = Settings(
      tolerance = params.tolerance(),
      debug = params.debug(),
      appId = spark.sparkContext.applicationId
    )
    val command = System.getProperty("sun.java.command")
    log(s"COMMAND|$command")
    log(s"INFO|scale=${S.scale}")
    val model = new PrecisionModel(S.scale)
    implicit val G: GeometryFactory = new GeometryFactory(model)
    log("TIME|Start")

    // Reading data...
    val edgesRDDA = read(params.input1())
    val nEdgesRDDA = edgesRDDA.count()
    log(s"INFO|edgesA=$nEdgesRDDA")
    val edgesRDDB = read(params.input2())
    val nEdgesRDDB = edgesRDDB.count()
    log(s"INFO|edgesB=$nEdgesRDDB")
    val edgesRDD = edgesRDDA.union(edgesRDDB).cache()
    val nEdgesRDD = edgesRDD.count()
    log(s"INFO|TotalEdges=$nEdgesRDD")

    val edgesRaw = new SpatialRDD[LineString]()
    edgesRaw.setRawSpatialRDD(edgesRDD)
    edgesRaw.analyze()
    edgesRaw.spatialPartitioning(GridType.KDBTREE, params.partitions())
    val edgesPartitioned1 = edgesRaw.spatialPartitionedRDD.rdd.cache()
    println(s"By Sedona KDTREE ${edgesPartitioned1.getNumPartitions}")
    edgesRaw.spatialPartitioning(GridType.QUADTREE, params.partitions())
    val edgesPartitioned2 = edgesRaw.spatialPartitionedRDD.rdd.cache()
    println(s"By Sedona QUADTREE ${edgesPartitioned2.getNumPartitions}")

    val envelope_area = getStudyArea(edgesRDD) // getStudyArea returns an Envelope...
    val paddedBoundary = new Envelope(envelope_area.getMinX, envelope_area.getMaxX + 0.01, envelope_area.getMinY, envelope_area.getMaxY + 0.01)
    val numPartitions = params.partitions()

    println(s"Input partitions: $numPartitions")

    val sample_size = SampleUtils.getSampleNumbers(numPartitions, nEdgesRDD)

    println(s"Sample size: $sample_size")

    val fraction = SampleUtils.computeFractionForSampleSize(sample_size, nEdgesRDD, withReplacement = false)

    println(s"Fraction: $fraction")

    val ( (kdtree, kdtree_space), kdtree_time) = timer{
      val sample = edgesRDD.sample(withReplacement = false, fraction, 42)
        .map(sample => (Random.nextDouble(), sample)).sortBy(_._1).map(_._2).collect()

      val max_items_per_cell = sample.length / numPartitions
      log(s"INFO|Kdtree|maxItemsPerCell|$max_items_per_cell")

      val kdtree = new KDBTree(max_items_per_cell, numPartitions, paddedBoundary)
      sample.foreach { sample =>
        kdtree.insert(sample.getEnvelopeInternal)
      }
      kdtree.assignLeafIds()
      val kn = kdtree.getLeaves.size()
      log(s"INFO|Kdtree|numPartitions|$kn")

      (kdtree, kn)
    }
    log(s"TIME|Kdtree|$kdtree_space|$kdtree_time")

    save(params.cpath()) {
      kdtree.getLeaves.asScala.map { case (id, envelope) =>
        val wkt = G.toGeometry(envelope)
        s"$wkt\t$id\n"
      }.toList
    }

    val ( (quadtree, quadtree_space), quadtree_time) = timer {
      val sample = edgesRDD.sample(withReplacement = false, fraction, 42).collect()
      val max_items_per_cell = sample.length / numPartitions
      log(s"INFO|Quadtree|maxItemsPerCell|$max_items_per_cell")
      val quadtree = new StandardQuadTree[Int](new QuadRectangle(paddedBoundary), 0, max_items_per_cell, numPartitions)
      sample.foreach { edge =>
        quadtree.insert(new QuadRectangle(edge.getEnvelopeInternal), 1)
      }
      quadtree.assignPartitionIds()
      quadtree.assignPartitionLineage()
      val qn = quadtree.getLeafZones.size()
      log(s"INFO|Quadtree|numPartitions|$qn")

      (quadtree, qn)
    }
    log(s"TIME|Quadtree|$quadtree_space|$quadtree_time")

    save(params.qpath()) {
      quadtree.getLeafZones.asScala.map { zone =>
        val wkt = G.toGeometry(zone.getEnvelope)
        val  id = zone.partitionId

        s"$wkt\t$id\n"
      }.toList
    }

    spark.close
  }
}
