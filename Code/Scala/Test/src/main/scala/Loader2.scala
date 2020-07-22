import org.slf4j.{LoggerFactory, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.utils.GeoSparkConf
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator
import com.vividsolutions.jts.io.WKTReader
import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel}
import com.vividsolutions.jts.geom.{Geometry, Polygon, LineString, LinearRing, Coordinate}
import com.vividsolutions.jts.geom.impl.CoordinateArraySequenceFactory
import org.locationtech.proj4j.{CRSFactory, CoordinateReferenceSystem, CoordinateTransform, CoordinateTransformFactory, ProjCoordinate}
import org.rogach.scallop._
import scala.io.Source

object Loader2 {
  def main(args: Array[String]): Unit = {
    implicit val logger: Logger = LoggerFactory.getLogger("myLogger")
    logger.info("Starting session...")
    val params = new LoaderConf(args)
    val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkVizKryoRegistrator].getName)
      .appName("GeoTemplate")
      .getOrCreate()
    val appId = spark.sparkContext.getConf.get("spark.app.id")
    val command = System.getProperty("sun.java.command")
    logger.info(s"${appId}|${command}")
    GeoSparkSQLRegistrator.registerAll(spark)
    import spark.implicits._
    logger.info("Starting session... Done!")

    logger.info(s"Reading ${params.input()}...")    
    val filename = params.input()
    val geometriesTXT = spark.read
      .option("header", true).option("delimiter", "\t")
      .csv(filename).rdd.cache()
    logger.info(s"Input polygons: ${geometriesTXT.count()}")
    logger.info(s"Reading ${params.input()}... Done!")

    logger.info("Removing duplicates...")
    val geometriesRaw = geometriesTXT.mapPartitions{ geoms =>
      val model = new PrecisionModel(1000)
      val geofactory = new GeometryFactory(model)
      val reader = new WKTReader(geofactory)
      geoms.map{ b =>
        val geom = reader.read(b.getString(0))
        val polygonRaw = geom.asInstanceOf[Polygon]
        val nGeoms = polygonRaw.getNumGeometries
        val rings = polygonRaw.getExteriorRing +:
          (0 until polygonRaw.getNumInteriorRing).map{ i =>
            polygonRaw.getInteriorRingN(i)
          }
        val coords = rings.map{ ring =>
          val coords = ring.getCoordinates.zipWithIndex.groupBy(_._1).map{ tuple =>
            val coord = tuple._1
            val index = tuple._2.map(_._2).sorted.head
            (index, coord)
          }.toList.sortBy(_._1).map(_._2).toArray
          coords :+ coords.head
        }
        val outer = geofactory.createLinearRing(coords.head)
        val inners = coords.tail.map(geofactory.createLinearRing).toArray
        geofactory.createPolygon(outer, inners)
      }
    }.cache()
    logger.info("Removing duplicates... Done!")
    logger.info(s"Output polygons: ${geometriesRaw.count}")

    
    logger.info(s"Saving to ${params.output()}...")
    if(params.partitions() < 2){
      val f = new java.io.PrintWriter(params.output())
      val wkt = geometriesRaw.collect().map{ polygon =>
        s"${polygon.toText()}\n"
      }.mkString("")
      f.write(wkt)
      f.close()
    } else {
      val geometries = geometriesRaw.distinct.map{_.toText()}
      geometries.cache()
      logger.info(s"Results: ${geometries.count()}")
      
      val output = params.output()
      geometries.toDF().write
        .mode(SaveMode.Overwrite)
        .option("header", false)
        .option("delimiter", "\t")
        .format("csv")
        .save(output)
    }
    logger.info(s"Saving to ${params.output()}... Done!")
    
    logger.info("Closing session...")
    spark.close()
    logger.info("Closing session... Done!")
  }
}
