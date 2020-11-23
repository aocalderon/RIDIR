import org.apache.spark.sql.SparkSession
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.functions.{min, max}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.geotools.referencing.CRS
import org.geotools.geometry.jts.JTS
import org.opengis.referencing.crs.CoordinateReferenceSystem
import org.opengis.referencing.operation.MathTransform
import com.vividsolutions.jts.geom.{GeometryFactory, Geometry, Coordinate, Polygon}
import com.vividsolutions.jts.io.WKTReader
import org.slf4j.{Logger, LoggerFactory}
import org.rogach.scallop.{ScallopConf, ScallopOption}
import scala.collection.JavaConverters._
import java.io.PrintWriter

object CoordTransformer {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val geofactory: GeometryFactory = new GeometryFactory()
  private val precision: Double = 0.0001
  private var startTime: Long = System.currentTimeMillis()

  def round(n: Double)(m: Int): Double = {
    val p = math.pow(10, m).toDouble
    math.round( n * p) / p
  }

  def clocktime = System.currentTimeMillis()

  def log(msg: String, timer: Long, n: Long, status: String): Unit ={
    logger.info("CT|%6.2f|%-50s|%6.2f|%6d|%s".format((clocktime-startTime)/1000.0, msg, (clocktime-timer)/1000.0, n, status))
  }

  def main(args: Array[String]): Unit = {
    val params = new CoordTransformerConf(args)
    val input = params.input()
    val output = params.output()
    val offset = params.offset()
    val cores = params.cores()
    val executors = params.executors()
    val sepsg = params.sepsg()
    val tepsg = params.tepsg()
    val master = params.local() match {
      case true  => s"local[${cores}]"
      case false => s"spark://${params.host()}:${params.port()}"
    }

    var timer = clocktime
    var stage = "Session start"
    log(stage, timer, 0, "START")
    val spark = SparkSession.builder()
      .config("spark.default.parallelism", 3 * cores * executors)
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .config("spark.scheduler.mode", "FAIR")
      .config("spark.cores.max", cores * executors)
      .config("spark.executor.cores", cores)
      .config("spark.kryoserializer.buffer.max.mb", "1024")
      .master(master)
      .appName("LATrajCleaner")
      .getOrCreate()
    import spark.implicits._
    startTime = spark.sparkContext.startTime
    log(stage, timer, 0, "END")

    timer = clocktime
    stage = "Read data"
    log(stage, timer, 0, "START")
    val sourcePolygons = spark.read.textFile(input).rdd.zipWithUniqueId().map{ case (line, id) =>
      val arr = line.split("\t")
      val userData = (0 until arr.size).filter(_ != offset).map(i => arr(i)) ++ List(id.toString())
      val wkt = arr(offset)
      val polygon = new WKTReader(geofactory).read(wkt)
      polygon.setUserData(userData.mkString("\t"))
      polygon
    }
    val nSourcePolygons = sourcePolygons.count()
    log(stage, timer, nSourcePolygons, "END")

    timer = clocktime
    stage = "Transform coordinates"
    log(stage, timer, 0, "START")
    val sourceCRS = CRS.decode(sepsg)
    val targetCRS = CRS.decode(tepsg)
    val lenient = true; // allow for some error due to different datums
    val transform = CRS.findMathTransform(sourceCRS, targetCRS, lenient);
    val targetPolygons = sourcePolygons.map{ polygon =>
      JTS.transform(polygon, transform).asInstanceOf[Polygon]
    }.cache()
    val nTargetPolygons = targetPolygons.count()
    log(stage, timer, nTargetPolygons, "END")

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
  val host:       ScallopOption[String]  = opt[String]  (default = Some("169.235.27.138"))
  val port:       ScallopOption[String]  = opt[String]  (default = Some("7077"))
  val sepsg:      ScallopOption[String]  = opt[String]  (default = Some("EPSG:4326"))
  val tepsg:      ScallopOption[String]  = opt[String]  (default = Some("EPSG:6423"))
  val cores:      ScallopOption[Int]     = opt[Int]     (default = Some(4))
  val executors:  ScallopOption[Int]     = opt[Int]     (default = Some(3))
  val partitions: ScallopOption[Int]     = opt[Int]     (default = Some(512))
  val local:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val debug:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val offset:     ScallopOption[Int]     = opt[Int]     (default = Some(1))

  verify()
}
