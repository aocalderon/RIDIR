import org.apache.spark.sql.SparkSession
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.functions.{min, max}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.geotools.referencing.CRS
import org.geotools.geometry.jts.JTS
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory, Geometry, Coordinate, Polygon}
import com.vividsolutions.jts.io.WKTReader
import org.slf4j.{Logger, LoggerFactory}
import org.rogach.scallop.{ScallopConf, ScallopOption}
import scala.collection.JavaConverters._
import java.io.PrintWriter

object CoordTransformer {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val tolerance: Double = 1e-5
  private val model: PrecisionModel = new PrecisionModel(1/tolerance)
  private val geofactory: GeometryFactory = new GeometryFactory()
  private var startTime: Long = System.currentTimeMillis()

  def clocktime = System.currentTimeMillis()

  def log(msg: String, timer: Long, n: Long, status: String): Unit ={
    logger.info("CT|%6.2f|%-50s|%6.2f|%6d|%s".format((clocktime-startTime)/1000.0, msg, (clocktime-timer)/1000.0, n, status))
  }

  def main(args: Array[String]): Unit = {
    val params = new CoordTransformerConf(args)
    val input = params.input()
    val output = params.output()
    val offset = params.offset()
    val sepsg = params.sepsg()
    val tepsg = params.tepsg()

    var timer = clocktime
    var stage = "Session start"
    log(stage, timer, 0, "START")
    val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()
    import spark.implicits._
    startTime = spark.sparkContext.startTime
    log(stage, timer, 0, "END")

    timer = clocktime
    stage = "Read data"
    log(stage, timer, 0, "START")
    val sourcePolygons = spark.read.textFile(input).rdd.zipWithUniqueId().map{ case (line, id) =>
      val reader = new WKTReader(geofactory)
      val arr = line.split("\t")
      val userData = (0 until arr.size).filter(_ != offset).map(i => arr(i)) ++ List(id.toString())
      val wkt = arr(offset)
      val polygon = reader.read(wkt)
      polygon.setUserData(userData.mkString("\t"))
      polygon
    }
    val nSourcePolygons = sourcePolygons.count()
    log(stage, timer, nSourcePolygons, "END")

    sourcePolygons.take(1).foreach(println)

    timer = clocktime
    stage = "Transform coordinates"
    log(stage, timer, 0, "START")
    val sourceCRS = CRS.decode(sepsg)
    val targetCRS = CRS.decode(tepsg)
    val lenient = true; // allow for some error due to different datums
    val mathTransform = CRS.findMathTransform(sourceCRS, targetCRS, lenient);

    val coords = Array(new Coordinate(10,10), new Coordinate(20, 20), new Coordinate(40, 25))
    val line = geofactory.createLineString(coords)
    val t = JTS.transform(line, mathTransform)
    println("T")
    println(t.toText)

    val targetPolygons = sourcePolygons.map{ polygon =>
      JTS.transform(polygon, mathTransform).asInstanceOf[Polygon]
    }.cache()
    val nTargetPolygons = targetPolygons.count()
    log(stage, timer, nTargetPolygons, "END")

    targetPolygons.take(1).foreach(println)

    timer = clocktime
    stage = "Save transformed polygons"
    log(stage, timer, 0, "START")
    val wkt = targetPolygons.map{ polygon =>
      val userData = polygon.getUserData.toString()
      
      s"${polygon.toText()}\t${userData}\n"
    }.collect()
    val f = new PrintWriter(output)
    f.write(wkt.mkString(""))
    f.close()
    val nWkt = wkt.size
    log(stage, timer, nWkt, "END")

    timer = clocktime
    stage = "Session close"
    log(stage, timer, 0, "START")
    spark.close()
    log(stage, timer, 0, "END")
  }
}

class CoordTransformerConf(args: Seq[String]) extends ScallopConf(args) {
  val input:      ScallopOption[String]  = opt[String]  (required = true)
  val output:     ScallopOption[String]  = opt[String]  (default  = Some("/tmp/output"))
  val sepsg:      ScallopOption[String]  = opt[String]  (default = Some("EPSG:4326"))
  val tepsg:      ScallopOption[String]  = opt[String]  (default = Some("EPSG:6423"))
  val partitions: ScallopOption[Int]     = opt[Int]     (default = Some(512))
  val local:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val debug:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val offset:     ScallopOption[Int]     = opt[Int]     (default = Some(0))

  verify()
}
