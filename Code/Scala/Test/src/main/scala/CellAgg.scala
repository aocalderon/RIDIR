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

object CellAgg{
  case class C(id: Int, lineage: String)

  def main(args: Array[String]): Unit = {
    implicit val logger: Logger = LoggerFactory.getLogger("myLogger")
    logger.info("Starting session...")
    val params = new CellAggConf(args)
    val delimiter = params.delimiter()
    implicit val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkVizKryoRegistrator].getName)
      .appName("Quadtree_Cell_Aggregator")
      .getOrCreate()
    val appId = spark.sparkContext.getConf.get("spark.app.id")
    val command = System.getProperty("sun.java.command")
    logger.info(s"${appId}|${command}")
    GeoSparkSQLRegistrator.registerAll(spark)
    implicit val model = new PrecisionModel(1/params.tolerance())
    implicit val geofactory = new GeometryFactory(model)
    import spark.implicits._
    logger.info("Starting session... Done!")

    val buffer = Source.fromFile(params.input())
    val Q = buffer.getLines.map{ line =>
      val fields = line.split(delimiter)
      val cid = fields(2).toInt
      val lin = fields(1)
      C(cid, lin)
    }.toList
    buffer.close


    spark.close()
  }  
}

class CellAggConf(args: Seq[String]) extends ScallopConf(args) {
  val input: ScallopOption[String]     = opt[String]  (required = true)
  val output: ScallopOption[String]    = opt[String]  (default = Some("/tmp/edgesClip.wkt"))
  val delimiter: ScallopOption[String] = opt[String]  (default = Some("\t"))
  val tolerance: ScallopOption[Double] = opt[Double]  (default = Some(1e-3))


  verify()
}
