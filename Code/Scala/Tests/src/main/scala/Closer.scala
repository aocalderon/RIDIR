package edu.ucr.dblab.tests

import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.rdd.RDD
import org.apache.spark.TaskContext

import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.sedona.core.serde.SedonaKryoRegistrator

import org.locationtech.jts.geom.{PrecisionModel, GeometryFactory}
import org.locationtech.jts.io.WKTReader

import org.slf4j.{LoggerFactory, Logger}
import org.rogach.scallop.{ScallopConf, ScallopOption}

import java.io.PrintWriter
import java.nio.file.Paths
import scala.annotation.tailrec

import edu.ucr.dblab.tests.GeoEncoders._
import edu.ucr.dblab.tests.PairingFunctions.anElegantPairingFunction

object Closer {
  def main(args: Array[String]): Unit = {
    implicit val logger: Logger = LoggerFactory.getLogger("myLogger")
    logger.info("Starting session...")
    implicit val params: CloserConf = new CloserConf(args)
    implicit val spark:SparkSession = SparkSession.builder()
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
    logger.info(s"Number of records   : ${centroidsRaw.count}")
    centroidsRaw.createOrReplaceTempView("centroidsRaw")

    logger.info("Computing the centroids...")
    val centroids = spark.sql("""
SELECT
  ST_GeomFromWKT(centroid) AS geom, geoid, edges, wkt
FROM
  centroidsRaw
""")
    centroids.createOrReplaceTempView("centroids")
    logger.info(s"Number of partitions: ${centroids.rdd.getNumPartitions}")

    logger.info("Computing the distances...")
    val distances = spark.sql(s"""
SELECT 
  geoid, ST_AsText(geom) as geom, edges, 
  ST_Distance(geom, ST_Point(${params.x()},${params.y()})) AS dist, wkt
FROM 
  centroids
""")
    distances.createOrReplaceTempView("distances")
    logger.info(s"Number of partitions: ${distances.rdd.getNumPartitions}")

    logger.info("Performing the cumulative sum...")    
    val limit = if(params.k() > 0){ s"LIMIT ${params.k()}" } else { "" }
    val result = spark.sql(s"""
SELECT
  wkt, geom, dist, edges, 
  sum(edges) OVER ( ORDER BY 
                          dist 
                    ROWS BETWEEN unbounded preceding AND CURRENT ROW ) AS cumulative_edges
FROM
  distances $limit
""").cache()
    logger.info(s"Number of partitions: ${result.rdd.getNumPartitions}")
    logger.info(s"Number of records   : ${result.count}")

    if(params.save()){
    logger.info("Saving cumulative data...")
      if(params.local()){
        val f = new PrintWriter(params.output())
        val wkt = result.collect.map{ row =>
          val wkt = row.getString(0)
          val centroid = row.getString(1)
          val distance = row.getDouble(2)
          val edges = row.getInt(3)
          val cumulative_edges = row.getLong(4)

          s"$wkt\t$centroid\t$distance\t$edges\t$cumulative_edges\n"
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
        }.write.mode(SaveMode.Overwrite).text(params.output())
      }
    }

    logger.info("Selecting cumulative data...")
    val data = result.rdd.map{ row =>
      val wkt        = row.getString(0)
      val centroid   = row.getString(1)
      val distance   = row.getDouble(2)
      val edges      = row.getInt(3)
      val cumulative = row.getLong(4)

      (wkt, edges, cumulative)
    }

    logger.info("Doing the splits...")    
    val total_edges = data.map{ case(w, e, c) => e }.reduce(_ + _)
    logger.info(s"Total edges: $total_edges")
    val parts = params.parts()
    val bound = total_edges * 1.0 / parts
    logger.info(s"Bound at: $bound")
    val bounds = (1 to parts).map{_ * bound}.toList
    logger.info(s"Bounds: ${bounds.mkString(" ")}")
    val rdds = splitByBound(bounds, data, List.empty[RDD[String]])

    logger.info("Saving data...")
    val filename = Paths.get(params.input()).getFileName.toString
    val tag = filename.split("\\.")(0)
    rdds.zipWithIndex.foreach{ case(rdd, id) =>
      rdd.toDF.write.mode(SaveMode.Overwrite).text(s"${params.output()}/${tag}${id}")
    }

    logger.info("Closing session...")
    spark.close
  }

  @tailrec
  def splitByBound(bounds: List[Double], data: RDD[(String, Int, Long)],
    splits: List[RDD[String]]): List[RDD[String]] = {

    bounds match {
      case Nil => splits
      case head :: tail => {
        val sample = data.filter{ case(wkt, edges, cumulative) => cumulative <= head }
          .map{ case(wkt, edges, cumulative) => wkt }
        splitByBound(tail, data, splits :+ sample)
      }
    }
  }
}

class CloserConf(args: Array[String]) extends ScallopConf(args) {
  val input:      ScallopOption[String]  = opt[String]  (required = true)
  val output:     ScallopOption[String]  = opt[String]  (required = true)
  val x:          ScallopOption[Double]  = opt[Double]  (default  = Some(10.0))
  val y:          ScallopOption[Double]  = opt[Double]  (default  = Some(10.0))
  val k:          ScallopOption[Int]     = opt[Int]     (default  = Some(100))
  val parts:      ScallopOption[Int]     = opt[Int]     (default  = Some(4))
  val partitions: ScallopOption[Int]     = opt[Int]     (default  = Some(128))
  val precision:  ScallopOption[Double]  = opt[Double]  (default  = Some(1000.0))
  val local:      ScallopOption[Boolean] = opt[Boolean] (default  = Some(false))
  val save:       ScallopOption[Boolean] = opt[Boolean] (default  = Some(false))
  verify()
}
