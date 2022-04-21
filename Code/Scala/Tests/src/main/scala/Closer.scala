package edu.ucr.dblab.tests

import org.apache.spark.sql.SparkSession
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.TaskContext

import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.sedona.core.serde.SedonaKryoRegistrator

import org.locationtech.jts.geom.{PrecisionModel, GeometryFactory}
import org.locationtech.jts.io.WKTReader

import org.slf4j.{LoggerFactory, Logger}
import org.rogach.scallop.{ScallopConf, ScallopOption}

import java.io.PrintWriter

import edu.ucr.dblab.tests.GeoEncoders._
import edu.ucr.dblab.tests.PairingFunctions.anElegantPairingFunction

object Closer {
  def main(args: Array[String]): Unit = {
    implicit val logger: Logger = LoggerFactory.getLogger("myLogger")
    logger.info("Starting session...")
    implicit val params: CloserConf = new CloserConf(args)
    implicit val spark:SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Closer")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)
      .getOrCreate()
    SedonaSQLRegistrator.registerAll(spark)
    import spark.implicits._

    implicit val model = new PrecisionModel(1000)
    implicit val geofactory = new GeometryFactory(model)

    logger.info(s"Reading ${params.input()}...")
    val centroidsRaw = spark.read.text(params.input()).repartition(params.partitions())
      .mapPartitions{ lines =>
        val partition_id = TaskContext.getPartitionId
        val reader = new WKTReader(geofactory)
        lines.zipWithIndex.map{ case(line, line_id) =>
          val wkt = line.getString(0).split("\t")(0)
          val geom = reader.read(wkt).getGeometryN(0)
          val edges = geom.getNumPoints + 1
          val centroid = geom.getCentroid
          val geoid = anElegantPairingFunction(partition_id, line_id)
          (centroid.toText(), geoid, edges, wkt)
        }
      }.toDF("centroid", "geoid", "edges", "wkt").cache()
    logger.info(s"Number of partitions: ${centroidsRaw.rdd.getNumPartitions}")
    centroidsRaw.createOrReplaceTempView("centroidsRaw")

    logger.info("Computing the centroids...")
    val centroids = spark.sql("""
SELECT
  ST_GeomFromWKT(centroid) AS geom, geoid, edges, wkt
FROM
  centroidsRaw
""").cache()
    centroids.createOrReplaceTempView("centroids")
    logger.info(s"Number of partitions: ${centroids.rdd.getNumPartitions}")

    logger.info("Computing the distances...")
    val distances = spark.sql(s"""
SELECT 
  geoid, ST_AsText(geom) as geom, edges, 
  ST_Distance(geom, ST_Point(${params.x()},${params.y()})) AS dist, wkt
FROM 
  centroids
""").cache()
    distances.createOrReplaceTempView("distances")
    logger.info(s"Number of partitions: ${distances.rdd.getNumPartitions}")

    logger.info("Performing the cumulative sum...")    
    val limit = if(params.k() > 0){ s"LIMIT ${params.k()}" } else { "" }
    val result = spark.sql(s"""
SELECT
  wkt, geom, dist, edges, sum(edges) OVER (ORDER BY dist) AS cumulative_edges
FROM
  distances $limit
""").cache()
    logger.info(s"Number of partitions: ${result.rdd.getNumPartitions}")

    logger.info("Saving the results...")
    if(false){
      val f = new PrintWriter(params.output())
      val wkt = result.collect.map{ row =>
        val wkt = row.getString(0)
        val centroid = row.getString(1)
        val distance = row.getDouble(2)
        val n   = row.getInt(3)
        val edges = row.getInt(4)
        val cumulative_edges = row.getLong(5)

        s"$wkt\t$centroid\t$distance\t$n\t$edges\t$cumulative_edges\n"
      }.mkString("")
      f.write(wkt)
      f.close
    } else {
      result.map{ row =>
        val wkt = row.getString(0)
        val centroid = row.getString(1)
        val distance = row.getDouble(2)
        val edges = row.getInt(3)
        val cumulative_edges = row.getLong(4)

        s"$wkt\t$centroid\t$distance\t$edges\t$cumulative_edges"
      }.write.mode(org.apache.spark.sql.SaveMode.Overwrite).text(params.output())
    }

    logger.info("Closing session...")
    spark.close
  }
}

class CloserConf(args: Array[String]) extends ScallopConf(args) {
  val input:      ScallopOption[String] = opt[String] (required = true)
  val output:     ScallopOption[String] = opt[String] (required = true)
  val x:          ScallopOption[Double] = opt[Double] (default  = Some(10.0))
  val y:          ScallopOption[Double] = opt[Double] (default  = Some(10.0))
  val k:          ScallopOption[Int]    = opt[Int]    (default  = Some(100))
  val partitions: ScallopOption[Int]    = opt[Int]    (default  = Some(128))
  val precision:  ScallopOption[Double] = opt[Double] (default  = Some(1000.0))
  verify()
}
