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
    implicit val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkVizKryoRegistrator].getName)
      .appName("GeoTemplate")
      .getOrCreate()
    val appId = spark.sparkContext.getConf.get("spark.app.id")
    val comma = System.getProperty("sun.java.command")
    logger.info(s"${appId}|${comma}")
    GeoSparkSQLRegistrator.registerAll(spark)
    val model = new PrecisionModel(params.precision())
    implicit val geofactory = new GeometryFactory(model)
    import spark.implicits._
    logger.info("Starting session... Done!")

    logger.info("Reading data...")
    val partitions = params.partitions()
    val input = params.input()
    val polysRDD = read(input)
    polysRDD.spatialPartitioning(GridType.QUADTREE, params.partitions())
    val regionWKT = "Polygon ((-150066.4127 -605327.0480, 310002.8938 -605327.0480, 310002.8938 -77440.1680, -150066.4127 -77440.1680, -150066.4127 -605327.0480))"
    val reader = new WKTReader(geofactory)
    val region = reader.read(regionWKT).asInstanceOf[Polygon]
    logger.info("Reading data... Done!")

    logger.info("Clipping...")
    val considerBoundaryIntersection = false 
    val usingIndex = false
    val result = RangeQuery.SpatialRangeQuery(polysRDD, region,
      considerBoundaryIntersection, usingIndex)
    logger.info("Clipping... Done!")

    
    logger.info("Saving to file...")
    val f = new java.io.PrintWriter(params.output())
    val data = result.rdd.map{ poly =>
      val wkt = poly.toText

      s"$wkt\n"
    }.collect
    f.write(data.mkString(""))
    f.close()
    logger.info(s"Saving to file... Done! (${data.size} polygons).")
    
    logger.info("Closing session...")
    spark.close()
    logger.info("Closing session... Done!")
  }

  def read(input: String)
    (implicit spark: SparkSession, geofactory: GeometryFactory, logger: Logger)
      : SpatialRDD[Polygon] = {
    import spark.implicits._
    val polys = spark.read
      .option("header", true).option("delimiter", "\t")
      .csv(input).rdd.mapPartitions{ it =>
        val reader = new WKTReader(geofactory)
        it.map{ row =>
          val wkt = row.getString(0)
          val lab = row.getString(1)
          val poly = reader.read(wkt).asInstanceOf[Polygon]
          poly.setUserData(lab)
          poly
        }
      }
    logger.info(s"Input polygons: ${polys.count()}")
    val raw = new SpatialRDD[Polygon]
    raw.setRawSpatialRDD(polys)
    raw.analyze()
    raw    
  }
}

class ClipperConf(args: Seq[String]) extends ScallopConf(args) {
  val input: ScallopOption[String]  = opt[String]  (required = true)
  val output: ScallopOption[String]  = opt[String]  (required = true)
  val partitions: ScallopOption[Int]     = opt[Int] (default = Some(960))
  val precision: ScallopOption[Double]     = opt[Double] (default = Some(1000.0))

  verify()
}
