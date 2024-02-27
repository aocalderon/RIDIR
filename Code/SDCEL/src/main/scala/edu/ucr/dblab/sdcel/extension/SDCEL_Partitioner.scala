package edu.ucr.dblab.sdcel.extension

import com.vividsolutions.jts.geom.{Envelope, GeometryFactory, PrecisionModel}
import edu.ucr.dblab.sdcel.Params
import edu.ucr.dblab.sdcel.Utils.{Settings, log, save}
import edu.ucr.dblab.sdcel.kdtree.KDBTree
import edu.ucr.dblab.sdcel.reader.PR_Utils.{getStudyArea, read}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.util.Random

object SDCEL_Partitioner {
  def main(args: Array[String]): Unit = {
    // Starting session...
    implicit val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()
    val params = new Params(args)
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

    val envelope_area = getStudyArea(edgesRDD) // getStudyArea returns an Envelope...
    val paddedBoundary = new Envelope(envelope_area.getMinX, envelope_area.getMaxX + 0.01, envelope_area.getMinY, envelope_area.getMaxY + 0.01)
    val numPartitions = params.partitions()
    val sample_size = SampleUtils.getSampleNumbers(numPartitions, nEdgesRDD)
    val fraction = SampleUtils.computeFractionForSampleSize(sample_size, nEdgesRDD, withReplacement = false)
    val sample = edgesRDD.sample(withReplacement = false, fraction, 42).map(sample => (Random.nextDouble(), sample)).sortBy(_._1).map(_._2).collect()
    val kdtree = new KDBTree(sample.length / numPartitions, params.maxlevel(), paddedBoundary)
    sample.foreach { sample =>
      kdtree.insert(sample.getEnvelopeInternal)
    }
    kdtree.assignLeafIds()
    val n = kdtree.getLeaves.size()
    log(s"Number of partitions: $n")

    save(params.cpath()) {
      kdtree.getLeaves.asScala.map { case (id, envelope) =>
        val wkt = G.toGeometry(envelope)
        s"$wkt\t$id\n"
      }.toList
    }

    spark.close
  }
}
