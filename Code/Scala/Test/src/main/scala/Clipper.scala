package edu.ucr.dblab

import org.slf4j.{LoggerFactory, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.rdd.RDD
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.spatialOperator.RangeQuery
import org.datasyslab.geospark.utils.GeoSparkConf
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator
import com.vividsolutions.jts.io.WKTReader
import com.vividsolutions.jts.geom.{Geometry, Polygon, Coordinate, GeometryFactory, PrecisionModel}
import org.rogach.scallop._
import scala.io.Source

object Clipper{
  def main(args: Array[String]): Unit = {
    implicit val logger: Logger = LoggerFactory.getLogger("myLogger")
    logger.info("Starting session...")
    val params = new ClipperConf(args)
    val delimiter = params.delimiter()
    implicit val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkVizKryoRegistrator].getName)
      .appName("GeoClipper")
      .getOrCreate()
    val appId = spark.sparkContext.getConf.get("spark.app.id")
    val command = System.getProperty("sun.java.command")
    logger.info(s"${appId}|${command}")
    GeoSparkSQLRegistrator.registerAll(spark)
    val model = new PrecisionModel(params.precision())
    implicit val geofactory = new GeometryFactory(model)
    import spark.implicits._
    logger.info("Starting session... Done!")

    logger.info("Reading data...")
    val partitions = params.partitions()
    val input = params.input()
    val polysRDD = read(input, delimiter)
    polysRDD.spatialPartitioning(GridType.QUADTREE, partitions)

    val regionWKT = Source.fromFile(params.region()).getLines.next()
    logger.info(s"Region: $regionWKT")
    val reader = new WKTReader(geofactory)
    val region = reader.read(regionWKT)
    logger.info("Reading data... Done!")

    logger.info("Clipping...")
    val considerBoundaryIntersection = false 
    val usingIndex = false
    val result = RangeQuery.SpatialRangeQuery(polysRDD, region,
      considerBoundaryIntersection, usingIndex)
    logger.info("Clipping... Done!")
    
    logger.info("Saving data...")
    val data = result.rdd.map{ geom =>
      val wkt = geom.toText
      val data = geom.getUserData
      s"$wkt$delimiter$data"
    }
    if(params.hdfs()){
      data.saveAsTextFile(params.output())
    } else {
      val f = new java.io.PrintWriter(params.output())
      f.write(data.collect.mkString("\n"))
      f.close()
    }
    logger.info(s"Saving to ${params.output()}... Done! (${data.count} polygons).")
    
    logger.info("Closing session...")
    spark.close()
    logger.info("Closing session... Done!")
  }

  def read(input: String, delimiter: String)
    (implicit spark: SparkSession, geofactory: GeometryFactory, logger: Logger)
      : SpatialRDD[Geometry] = {
    import spark.implicits._
    val geoms = spark.read
      .text(input).rdd.mapPartitions{ it =>
        val reader = new WKTReader(geofactory)
        it.map{ row =>
          val fields = row.getString(0).split(delimiter)
          val wkt = fields(0)
          val lab = fields.tail.mkString(delimiter)
          val poly = reader.read(wkt)
          poly.setUserData(lab)
          poly
        }
      }
    logger.info(s"Input geometries: ${geoms.count()}")
    val raw = new SpatialRDD[Geometry]
    raw.setRawSpatialRDD(geoms)
    raw.analyze()
    raw    
  }
}

class ClipperConf(args: Seq[String]) extends ScallopConf(args) {
  val input: ScallopOption[String]     = opt[String]  (required = true)
  val output: ScallopOption[String]    = opt[String]  (default = Some("/tmp/edgesClip.wkt"))
  val partitions: ScallopOption[Int]   = opt[Int]     (default = Some(1024))
  val precision: ScallopOption[Double] = opt[Double]  (default = Some(1000.0))
  val delimiter: ScallopOption[String] = opt[String]  (default = Some("\t"))
  val region: ScallopOption[String]    = opt[String]  (default = Some("/tmp/region.wkt"))
  val hdfs: ScallopOption[Boolean]     = opt[Boolean] (default = Some(false))

  verify()
}
