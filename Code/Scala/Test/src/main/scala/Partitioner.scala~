import org.slf4j.{LoggerFactory, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.utils.GeoSparkConf
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator
import com.vividsolutions.jts.io.WKTReader
import com.vividsolutions.jts.geom.{Geometry, Polygon, Coordinate, GeometryFactory, PrecisionModel}
import org.locationtech.proj4j.{CRSFactory, CoordinateReferenceSystem, CoordinateTransform, CoordinateTransformFactory, ProjCoordinate}
import org.rogach.scallop._
import scala.io.Source

object Clipper{
  def main(args: Array[String]): Unit = {
    implicit val logger: Logger = LoggerFactory.getLogger("myLogger")
    logger.info("Starting session...")
    val params = new ClipperConf(args)
    val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkVizKryoRegistrator].getName)
      .appName("GeoTemplate")
      .getOrCreate()
    val appId = spark.sparkContext.getConf.get("spark.app.id")
    val comma = System.getProperty("sun.java.command")
    logger.info(s"${appId}|${comma}")
    GeoSparkSQLRegistrator.registerAll(spark)
    import spark.implicits._
    logger.info("Starting session... Done!")

    logger.info("Reading data...")
    val partitions = params.partitions()
    val wkt = Source.fromFile(params.clip()).getLines.next
    val usa = spark.sparkContext.broadcast(wkt)
    
    val filename = params.input()
    val geometriesTXT = spark.read
      .option("header", true).option("delimiter", "\t")
      .csv(filename).rdd.cache()
    logger.info(s"Input polygons: ${geometriesTXT.count()}")
    logger.info("Reading data... Done!")

    logger.info("Clipping...")
    val geometriesRaw = geometriesTXT.mapPartitions{ bs =>
      val continental = usa.value
      val model = new PrecisionModel(1000)
      val geofactory = new GeometryFactory(model)
      val reader = new WKTReader(geofactory)
      val usaJTS = reader.read(continental)
      bs.map{ b =>
        val geom = reader.read(b.getString(0))
        val polygon = geom.asInstanceOf[Polygon]
        
        (polygon, usaJTS.intersects(polygon))
      }.filter(_._2).map(_._1)
    }.cache()
    logger.info("Clipping... Done!")
    logger.info(s"Output polygons: ${geometriesRaw.count}")

    
    logger.info("Saving to file...")
    if(partitions < 2){
      val f = new java.io.PrintWriter(params.output())
      val wkt = geometriesRaw.collect().map{ polygon =>
        s"${polygon.toText()}\n"
      }.mkString("")
      f.write(wkt)
      f.close()
    } else {
      val buildingsRDD = new SpatialRDD[Polygon]
      buildingsRDD.setRawSpatialRDD(geometriesRaw)
      buildingsRDD.analyze()
      buildingsRDD.spatialPartitioning(GridType.QUADTREE, partitions)
      
      val output = params.output()
      buildingsRDD.spatialPartitionedRDD.rdd
        .map(_.toText).toDF("geom").write
        .mode(SaveMode.Overwrite)
        .option("delimiter", "\t")
        .format("csv")
        .save(output)
    }
    logger.info("Saving to file... Done!")
    
    logger.info("Closing session...")
    spark.close()
    logger.info("Closing session... Done!")
  }
}

class ClipperConf(args: Seq[String]) extends ScallopConf(args) {
  val input: ScallopOption[String]  = opt[String]  (required = true)
  val output: ScallopOption[String]  = opt[String]  (required = true)
  val clip: ScallopOption[String]  = opt[String]  (required = true)
  val partitions: ScallopOption[Int]     = opt[Int] (default = Some(960))

  verify()
}
