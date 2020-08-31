import scala.collection.JavaConverters._
import com.vividsolutions.jts.geom.{Coordinate, Envelope, Geometry}
import com.vividsolutions.jts.geom.{LineString, LinearRing, Point, Polygon}
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{Row, Dataset, SparkSession}
import org.apache.spark.sql.functions
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.spatialPartitioning.quadtree._
import org.slf4j.{Logger, LoggerFactory}
import SingleLabelChecker._

object SingleLabelCheckerTest{
  case class FaceT(wkt: String, id: String, pid: Int = -1)

  def main(args: Array[String]): Unit = {
    implicit val logger: Logger = LoggerFactory.getLogger("myLogger")
    val model = new PrecisionModel(1000)
    val geofactory = new GeometryFactory(model)

    // Starting session...
    logger.info("Starting session...")
    val appName = "SingleLabelCheckerTester"
    implicit val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .appName(appName)
      .getOrCreate()
    import spark.implicits._
    logger.info("Starting session... Done!")

    logger.info("Reading data...")
    val input = "tmp/faces"
    val faces = spark.read.text(input).map{ row =>
      val arr = row.getString(0).split("\t")
      val reader = new WKTReader(geofactory)
      val face = reader.read(arr(0)).asInstanceOf[Polygon]
      val id = arr(1)
      val pid = arr(2).toInt

      FaceT(face.toText, id, pid)
    }.repartition(23202, $"pid").cache()
    logger.info("Reading data... Done!")
    val npartitions = faces.rdd.getNumPartitions
    logger.info(s"Number of partitions: $npartitions")

    faces.rdd.take(5).foreach{println}

    spark.close()
  }
}
