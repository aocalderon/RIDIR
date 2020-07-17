package edu.ucr.dblab

import org.apache.spark.serializer.KryoSerializer
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel}
import com.vividsolutions.jts.geom.{Polygon}
import com.vividsolutions.jts.io.WKTReader
import org.rogach.scallop.ScallopConf
import org.slf4j.{Logger, LoggerFactory}
import scala.util.Random
import edu.ucr.dblab.Utils.save

object Tester {
  implicit val logger = LoggerFactory.getLogger("myLogger")
  implicit val geofactory = new GeometryFactory(new PrecisionModel(1000))

  def main(args: Array[String]): Unit = {
    val params = new TesterConf(args)
    val input = params.input()
    val offset = params.offset()
    val partitions = params.partitions()

    val appName = "Tester"
    implicit val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .appName(appName)
      .getOrCreate()
    import spark.implicits._

    val geometries = spark.read.textFile(input).rdd
      .zipWithUniqueId().flatMap{ case (line, i) =>
        val arr = line.split("\t")
        val userData = s"$i" +: (0 until arr.size).filter(_ != offset).map(i => arr(i))
        val wkt =  arr(offset).replaceAll("\"", "")
        val geometry = new WKTReader(geofactory).read(wkt)
        geometry.setUserData(userData.mkString("\t"))
        val geometryType = geometry.getGeometryType
        geometryType match {
          case "Polygon" => List(geometry)
          case "MultiPolygon" => {
            val n = geometry.getNumGeometries
            (0 until n).map{ i => geometry.getGeometryN(i) }
          }
        }
      }.repartition(partitions)
    val nGeometries = geometries.count()

    logger.info(s"Number of polygons: $nGeometries")
    geometries.map{_.getGeometryType}.toDF("type")
      .groupBy("type")
      .agg(functions.count(functions.lit(1)).alias("n"))
      .show()

    geometries.map{ geom =>
      val wkt = geom.toText
      val userData = geom.getUserData.toString()

      s"${wkt}\t${userData}"
    }.saveAsTextFile(params.output())
  }
}

class TesterConf(args: Seq[String]) extends ScallopConf(args) {
  val input = opt[String](default = Some(""))
  val output = opt[String](default = Some(""))
  val offset = opt[Int](default = Some(0))
  val partitions = opt[Int](default = Some(288))

  verify()
}
