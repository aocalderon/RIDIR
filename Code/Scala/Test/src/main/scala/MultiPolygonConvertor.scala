import org.slf4j.{LoggerFactory, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.utils.GeoSparkConf
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import com.vividsolutions.jts.io.WKTReader
import com.vividsolutions.jts.geom.{Geometry, Polygon, Coordinate, GeometryFactory, PrecisionModel}
import org.rogach.scallop._

object MultiPolygonConverter {
  def main(args: Array[String]): Unit = {
    implicit val logger: Logger = LoggerFactory.getLogger("myLogger")
    logger.info("Starting session...")
    implicit val params = new MCConf(args)
    implicit val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .appName("GeoTemplate")
      .getOrCreate()
    implicit val model = new PrecisionModel(1 / params.tolerance())
    implicit val geofactory = new GeometryFactory(model)
    import spark.implicits._
    logger.info("Starting session... Done!")

    val input = params.input()
    logger.info(s"Reading ${input}...")
    val partitions = params.partitions()
    val geometries = spark.read
      .option("header", true)
      .option("delimiter", "\t")
      .csv(input).rdd
      .repartition(partitions).cache()
    logger.info(s"Reading ${input}... Done!")
    logger.info(s"Number of records: ${geometries.count()}")

    logger.info("Extracting polygons...")
    val shells = geometries.mapPartitions{ it =>
      val reader = new WKTReader(geofactory)
      it.map{ row =>
        val wkt = row.getString(0)
        val userData = (1 until row.length).map{ i => row.getString(i) }.mkString("\t")
        val geometry = reader.read(wkt)
        val n = geometry.getNumGeometries
        val shells = (0 until n).map{ i =>
          val wkt = geometry.getGeometryN(i).toText
          s"$wkt\t${i}_$userData\n"
        }
        shells
      }.flatten
    }.cache()
    logger.info("Extracting polygons... Done!")

    val output = params.output()
    logger.info(s"Saving to ${output}...")
    val f = new java.io.PrintWriter(output)
    val wkt = shells.collect().mkString("")
    f.write(wkt)
    f.close()
    logger.info(s"Saving to ${output}... Done!")
    logger.info(s"Number of records: ${shells.count()}")

    logger.info("Closing session...")
    spark.close()
    logger.info("Closing session... Done!")
  }
}

class MCConf(args: Seq[String]) extends ScallopConf(args) {
  val input:      ScallopOption[String] = opt[String]  (required = true)
  val output:     ScallopOption[String] = opt[String]  (required = true)
  val partitions: ScallopOption[Int]    = opt[Int]     (default = Some(960))
  val tolerance:  ScallopOption[Double] = opt[Double]  (default = Some(1e-3))

  verify()
}
