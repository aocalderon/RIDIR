import org.slf4j.{LoggerFactory, Logger}
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.spatialRDD.{SpatialRDD, PolygonRDD, LineStringRDD}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialPartitioning.{KDBTree, KDBTreePartitioner}
import com.vividsolutions.jts.geom.GeometryFactory
import com.vividsolutions.jts.geom.{Geometry, Envelope, Polygon, LineString}
import com.vividsolutions.jts.io.WKTReader
//import org.locationtech.jts.geom.GeometryFactory
//import org.locationtech.jts.geom.{Geometry, Envelope, Polygon}
//import org.locationtech.jts.io.WKTReader
import scala.collection.JavaConverters._

object DCEL{
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val geofactory: GeometryFactory = new GeometryFactory();
  private val reader = new WKTReader(geofactory)
  private val precision: Double = 0.001
  private val startTime: Long = 0L

  def clocktime = System.currentTimeMillis()

  def log(msg: String, timer: Long, n: Long, status: String): Unit ={
    logger.info("DCEL|%6.2f|%-50s|%6.2f|%6d|%s".format((clocktime-startTime)/1000.0, msg, (clocktime-timer)/1000.0, n, status))
  }

  def saveLineString(lines: RDD[LineString], filename: String): Unit = {
    val f = new java.io.PrintWriter(filename)
    val wkt = lines.map{ line =>
      val id = line.getUserData.toString()
      s"${id}\t${line.toText()}\n"
    }.collect.mkString("")
    f.write(wkt)
    f.close()
  }

  /***
   * The main function...
   **/
  def main(args: Array[String]) = {
    val params: DCELConf = new DCELConf(args)
    val cores = params.cores()
    val executors = params.executors()
    val input = params.input()
    val partitions = params.partitions()
    val master = params.local() match {
      case true  => s"local[${cores}]"
      case false => s"spark://${params.host()}:${params.port()}"
    }
    logger.info(s"${params.local()}")
    logger.info(master)

    // Starting session...
    var timer = clocktime
    var stage = "Session started"
    log(stage, timer, 0, "START")
    val spark = SparkSession.builder()
      .config("spark.default.parallelism", 3 * cores * executors)
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .config("spark.scheduler.mode", "FAIR")
      .config("spark.cores.max", cores * executors)
      .config("spark.executor.cores", cores)
      .master(master)
      .appName("DCEL")
      .getOrCreate()
    import spark.implicits._
    val appID = spark.sparkContext.applicationId
    val startTime = spark.sparkContext.startTime
    log(stage, timer, 0, "END")

    // Reading data...
    timer = System.currentTimeMillis()
    stage = "Data read"
    log(stage, timer, 0, "START")
    val polygonRDD = new SpatialRDD[Polygon]()
    val polygons = spark.read.option("header", "false").option("delimiter", "\t")
      .csv(input).rdd.zipWithUniqueId().map{ row =>
        val polygon = reader.read(row._1.getString(0)).asInstanceOf[Polygon]
        polygon.setUserData(s"${row._2}")
        polygon
      }
    polygonRDD.setRawSpatialRDD(polygons)
    val nPolygons = polygons.count()
    log(stage, timer, nPolygons, "END")

    // Getting segments...
    timer = clocktime
    stage = "Segments gotten"
    log(stage, timer, 0, "START")
    polygonRDD.analyze()
    polygonRDD.spatialPartitioning(GridType.EQUALGRID, partitions)
    val segmentsRDD = new SpatialRDD[LineString]()
    val segments = polygonRDD.spatialPartitionedRDD.rdd.flatMap{ polygon =>
      val id = polygon.getUserData.toString()
      val coords = polygon.getExteriorRing.getCoordinateSequence.toCoordinateArray().toList
      coords.zip(coords.tail ++ List(coords.head)).map{ pair =>
        val edges = List(pair._1, pair._2).toArray
        val segment = geofactory.createLineString(edges)
        segment.setUserData(s"${id}")
        segment
      }
    }
    segmentsRDD.setRawSpatialRDD(segments)
    val nSegments = segments.count()
    log(stage, timer, nSegments, "END")

    saveLineString(segments, "/tmp/segments.wkt")

    // Closing session...
    timer = System.currentTimeMillis()
    stage = "Session closed"
    log(stage, timer, 0, "START")
    spark.close()
    log(stage, timer, 0, "END")
  }  
}

class DCELConf(args: Seq[String]) extends ScallopConf(args) {
  val input:      ScallopOption[String]  = opt[String]  (required = true)
  val host:       ScallopOption[String]  = opt[String]  (default = Some("169.235.27.138"))
  val port:       ScallopOption[String]  = opt[String]  (default = Some("7077"))
  val cores:      ScallopOption[Int]     = opt[Int]     (default = Some(4))
  val executors:  ScallopOption[Int]     = opt[Int]     (default = Some(3))
  val grid:       ScallopOption[String]  = opt[String]  (default = Some("KDBTREE"))
  val index:      ScallopOption[String]  = opt[String]  (default = Some("QUADTREE"))
  val partitions: ScallopOption[Int]     = opt[Int]     (default = Some(512))
  val local:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val debug:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))

  verify()
}

