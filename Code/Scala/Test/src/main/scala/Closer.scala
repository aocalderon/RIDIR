package edu.ucr.dblab

import org.slf4j.{LoggerFactory, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.rdd.RDD
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.spatialOperator.KNNQuery
import org.datasyslab.geospark.utils.GeoSparkConf
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator
import com.vividsolutions.jts.io.WKTReader
import com.vividsolutions.jts.geom.{Point, Coordinate, GeometryFactory, PrecisionModel}
import org.rogach.scallop._
import scala.collection.JavaConverters._

object Closer{
  def main(args: Array[String]): Unit = {
    implicit val logger: Logger = LoggerFactory.getLogger("myLogger")
    logger.info("Starting session...")
    val params = new CloserConf(args)
    implicit val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkVizKryoRegistrator].getName)
      .appName("Closer")
      .getOrCreate()
    import spark.implicits._
    val appId = spark.sparkContext.getConf.get("spark.app.id")

    implicit val model = new PrecisionModel(params.precision())
    implicit val geofactory = new GeometryFactory(model)

    val polys = spark.read.text(params.input()).rdd.map{ line =>
      line.getString(0).split("\t")(0)
    }.zipWithIndex.toDF("wkt", "id")

    val centroidsRDD = polys.rdd.mapPartitions{ rows =>
      val reader = new WKTReader(geofactory)
      rows.map{ row =>
        val wkt = row.getString(0)
        val id = row.getLong(1)
        val polygon  =reader.read(wkt)
        val nedges = polygon.getGeometryN(0).getNumPoints
        val centroid = polygon.getCentroid
        val info = s"$id\t$nedges"
        centroid.setUserData(info)
        centroid
      }
    }

    val centroids = new SpatialRDD[Point]
    centroids.setRawSpatialRDD(centroidsRDD)
    centroids.analyze
    logger.info(params.input())
    logger.info(s"Number of records: ${centroids.rawSpatialRDD.count}")

    val point = geofactory.createPoint(new Coordinate(params.x(), params.y()))

    import org.apache.spark.sql.functions._
    val x = params.x()
    val y = params.y()
    val result = centroidsRDD.mapPartitions{ cs =>
      val p = geofactory.createPoint(new Coordinate(x, y))
      cs.map{ c =>
        val d = p.distance(c)
        val info = c.getUserData.asInstanceOf[String].split("\t")
        val id = info(0).toLong
        val nedges = info(1).toLong
        (c.toText, id, nedges, d)
      }
    }.toDF("point", "id", "nedges", "dist")

    val data = polys.join(result, "id").select("wkt", "id", "dist", "nedges")
      .orderBy(asc("dist")).map{ p =>
        val wkt = p.getString(0)
        val id = p.getLong(1)
        val d = p.getDouble(2)
        val ne = p.getLong(3)
        s"$wkt\t$id\t$d\t$ne\n"
      }.head(params.k())

    val f = new java.io.FileWriter(params.output())
    f.write(data.mkString(""))
    f.close
    logger.info(s"Saved ${data.size} records at ${params.output()}.")

    spark.close
  }
}

class CloserConf(args: Seq[String]) extends ScallopConf(args) {
  val input: ScallopOption[String]     = opt[String]  (required = true)
  val x: ScallopOption[Double] = opt[Double] (default = Some(10.0))
  val y: ScallopOption[Double] = opt[Double] (default = Some(10.0))
  val k: ScallopOption[Int]   = opt[Int] (default = Some(100))
  val output: ScallopOption[String]    = opt[String]  (required = true)
  val partitions: ScallopOption[Int]   = opt[Int] (default = Some(960))
  val precision: ScallopOption[Double] = opt[Double] (default = Some(1000.0))

  verify()
}
