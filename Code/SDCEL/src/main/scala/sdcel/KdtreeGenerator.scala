package sdcel

import com.vividsolutions.jts.geom.{GeometryFactory, LineString, Polygon, PrecisionModel}
import com.vividsolutions.jts.io.WKTReader
import edu.ucr.dblab.sdcel.Params
import edu.ucr.dblab.sdcel.QuadtreeGenerator.{envelope2polygon, getLineStrings, save}
import edu.ucr.dblab.sdcel.Utils.{Settings, log}
import edu.ucr.dblab.sdcel.kdtree.KDBTree
import edu.ucr.dblab.sdcel.reader.PR_Utils
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.util.Random

object KdtreeGenerator {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def read(input: String)(implicit spark: SparkSession, G: GeometryFactory): RDD[LineString] = {

    val polys = spark.read.textFile(input).rdd.cache
    logger.info(s"TIME|npolys${polys.count()}")
    val edgesRaw = polys.mapPartitions{ lines=>
      val reader = new WKTReader(G)
      lines.flatMap{ line0 =>
        val line = line0.split("\t")(0)
        val geom = reader.read(line.replaceAll("\"", ""))
        (0 until geom.getNumGeometries).map{ i =>
          geom.getGeometryN(i).asInstanceOf[Polygon]
        }
      }
    }.zipWithIndex.flatMap{ case(polygon, id) =>
      getLineStrings(polygon, id)
    }.cache

    edgesRaw
  }

  def main(args: Array[String]): Unit = {
    // Starting session...
    val params = new Params(args)
    implicit val spark: SparkSession = SparkSession.builder()
      .master(params.master())
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()
    implicit val S: Settings = Settings(
      tolerance = params.tolerance(),
      debug = params.debug(),
      local = params.local(),
      appId = spark.sparkContext.applicationId
    )
    val command = System.getProperty("sun.java.command")
    log(s"COMMAND|$command")
    log(s"INFO|scale=${S.scale}")
    implicit val M: PrecisionModel = new PrecisionModel(S.scale)
    implicit val G: GeometryFactory = new GeometryFactory(M)
    log("TIME|Start")

    // Reading data...
    val edgesRDDA = read(params.input1())
    val nEdgesRDDA = edgesRDDA.count()
    log(s"INFO|edgesA=$nEdgesRDDA")
    val edgesRDDB = read(params.input2())
    val nEdgesRDDB = edgesRDDB.count()
    log(s"INFO|edgesB=$nEdgesRDDB")
    val edgesRDD = edgesRDDA.union( edgesRDDB )
    val boundary = PR_Utils.getStudyArea(edgesRDD)
    boundary.expandBy(0.01)
    log("TIME|Read")

    // Creating kdtree...
    val partitions = params.partitions()

    // Sampling...
    val sample = edgesRDD.sample(withReplacement = false, params.fraction(), 42)
      .map(sample => (Random.nextDouble(), sample)).sortBy(_._1).map(_._2)
      .collect()

    // Feeding...
    val max_items_per_cell = sample.length / partitions
    val kdtree = new KDBTree(max_items_per_cell, partitions, boundary)
    sample.foreach { sample =>
      kdtree.insert(sample.getEnvelopeInternal)
    }
    kdtree.assignLeafIds()
    kdtree.dropElements()
    val kn = kdtree.getLeaves.size()
    log(s"TIME|Kdtree|creation|$partitions|$kn")

    // Saving the boundary...
    save{params.epath()}{
      val wkt = envelope2polygon(boundary).toText
      List(s"$wkt\n")
    }
    // Saving the kdtree...
    save{params.kpath()}{
      kdtree.getLeaves.asScala.map{ case(id, envelope) =>
        val wkt = G.toGeometry(envelope).toText

        s"$wkt\t$id\n"
      }.toList
    }

    spark.close
    log("TIME|Close")
  }

}
