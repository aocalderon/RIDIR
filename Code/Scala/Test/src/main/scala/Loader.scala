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

object Loader {
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
    val input = params.input()
    val partitions = params.partitions()
    val geometriesTXT = spark.read
      .option("header", true)
      .csv(input)
      .repartition(partitions)
      .cache()
    geometriesTXT.show()
    logger.info(s"Input polygons: ${geometriesTXT.count()}")
    logger.info(s"Reading ${params.input()}... Done!")

    logger.info("Cleaning...")
    val offset = params.offset()
    val geometriesRaw = geometriesTXT.rdd.mapPartitions{ wkts =>
      val model = new PrecisionModel(1000)
      val geofactory = new GeometryFactory(model)
      val reader = new WKTReader(geofactory)
      wkts.flatMap{ wkt =>
        val geoms = reader.read(wkt.getString(offset))
          (0 until geoms.getNumGeometries).map{ n =>
            val geom = geoms.getGeometryN(n)
            geom.asInstanceOf[Polygon]
          }
      }
    }.cache()
    logger.info("Cleaning... Done!")
    geometriesRaw.map(_.toText()).toDF().show()
    logger.info(s"Output polygons: ${geometriesRaw.count()}")

    
    logger.info(s"Saving to ${params.output()}...")
    if(partitions < 2){
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

class LoaderConf(args: Seq[String]) extends ScallopConf(args) {
  val input: ScallopOption[String] = opt[String]  (required = true)
  val output: ScallopOption[String] = opt[String]  (required = true)
  val offset: ScallopOption[Int] = opt[Int] (default = Some(0))
  val partitions: ScallopOption[Int] = opt[Int] (default = Some(1080))

  verify()
}
