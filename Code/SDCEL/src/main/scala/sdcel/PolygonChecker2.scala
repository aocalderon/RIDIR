package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geom.{Geometry, Polygon, LinearRing}
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.slf4j.{Logger, LoggerFactory}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.enums.GridType

import collection.JavaConverters._
import edu.ucr.dblab.sdcel.DCELBuilder2.save

object PolygonChecker2 {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")
 
  val SCALE = 10e13

  case class Info(id: Long, code: String){
    override def toString = s"$id\t$code"
  }

  def main(args: Array[String]): Unit = {
    logger.info("Starting session.")
    val model = new PrecisionModel(SCALE)
    implicit val geofactory = new GeometryFactory(model)

    implicit val spark = SparkSession.builder()
        .config("spark.serializer",classOf[KryoSerializer].getName)
        .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
        .getOrCreate()
    import spark.implicits._

    logger.info("Reading data...")
    val polygonsRAW = read("gadm/raw/level1")

    logger.info("Partitioning data...")
    val polygonsSP = new SpatialRDD[Polygon]()
    polygonsSP.setRawSpatialRDD(polygonsRAW)
    polygonsSP.analyze()
    polygonsSP.spatialPartitioning(GridType.QUADTREE, 1024)
    val polygonsRDD = polygonsSP.spatialPartitionedRDD.rdd.cache

    logger.info("Detecting overlaps...")
    val pairsRDD = polygonsRDD.mapPartitions{ polygonsIt =>
      val polygons = polygonsIt.toList

      val pairs = for{
        a <- polygons
        b <- polygons if a.relate(b, "200000000") && info(a).id < info(b).id
      } yield {
        (a, b)
      }

      pairs.toIterator
    }.cache
    val nPairsRDD = pairsRDD.count

    logger.info(s"Overlapping polygons: $nPairsRDD")
    pairsRDD.take(10).map{ case(a, b) =>
      s"${text(a)}\t${text(b)}\t${info(a)}\t${info(b)}"
    }.foreach(println)

    spark.close
  }

  def info(p: Polygon): Info = p.getUserData.asInstanceOf[Info]
  def text(p: Polygon): String = p.toText

  def read(input: String)
    (implicit spark: SparkSession, geofactory: GeometryFactory): RDD[Polygon] = {

    val polygonsRDD = spark.read.textFile(input).rdd
      .mapPartitions{ lines =>
        val reader = new WKTReader(geofactory)
        lines.map{ line =>
          val arr = line.split("\t")
          val wkt = arr(0)
          val polygon = reader.read(wkt).asInstanceOf[Polygon]
          val id = arr(1).toLong
          val code = arr(2)
          polygon.setUserData(Info(id, code))
          polygon
        }
      }

    polygonsRDD
  }
}