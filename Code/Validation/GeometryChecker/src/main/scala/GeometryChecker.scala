import org.slf4j.{LoggerFactory, Logger}
import org.rogach.scallop._
import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.spatialRDD.{SpatialRDD, PolygonRDD, LineStringRDD}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialPartitioning.{KDBTree, KDBTreePartitioner}
import com.vividsolutions.jts.operation.buffer.BufferParameters
import com.vividsolutions.jts.geom.GeometryFactory
import com.vividsolutions.jts.geom.{Geometry, Envelope, Coordinate,  Polygon, LinearRing, LineString, MultiPolygon}
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence
import com.vividsolutions.jts.algorithm.{RobustCGAlgorithms, CGAlgorithms}
import com.vividsolutions.jts.io.WKTReader
import org.geotools.geometry.jts.GeometryClipper
import scala.collection.JavaConverters._
import scala.collection.mutable.{ListBuffer, TreeSet, ArrayBuffer, HashSet}

object GeometryChecker{
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val geofactory: GeometryFactory = new GeometryFactory();
  private val reader = new WKTReader(geofactory)
  private val precision: Double = 0.0001
  private val startTime: Long = 0L

  def clocktime = System.currentTimeMillis()

  def log(msg: String, timer: Long, n: Long, status: String): Unit ={
    logger.info("GeomChecker|%6.2f|%-50s|%6.2f|%6d|%s".format((clocktime-startTime)/1000.0, msg, (clocktime-timer)/1000.0, n, status))
  }

  def envelope2Polygon(e: Envelope): Polygon = {
    val minX = e.getMinX()
    val minY = e.getMinY()
    val maxX = e.getMaxX()
    val maxY = e.getMaxY()
    val p1 = new Coordinate(minX, minY)
    val p2 = new Coordinate(minX, maxY)
    val p3 = new Coordinate(maxX, maxY)
    val p4 = new Coordinate(maxX, minY)
    val coordArraySeq = new CoordinateArraySequence( Array(p1,p2,p3,p4,p1), 2)
    val ring = new LinearRing(coordArraySeq, geofactory)
    new Polygon(ring, null, geofactory)
  }

  /***
   * The main function...
   **/
  def main(args: Array[String]) = {
    val params: GeometryCheckerConf = new GeometryCheckerConf(args)
    val cores = params.cores()
    val executors = params.executors()
    val input = params.input()
    val offset = params.offset()
    val partitions = params.partitions()
    val debug = params.debug()
    val master = params.local() match {
      case true  => s"local[${cores}]"
      case false => s"spark://${params.host()}:${params.port()}"
    }
    val gridType = params.grid() match {
      case "EQUALTREE" => GridType.EQUALGRID
      case "QUADTREE"  => GridType.QUADTREE
      case "KDBTREE"   => GridType.KDBTREE
    }

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
      .appName("GeometryChecker")
      .getOrCreate()
    import spark.implicits._
    val appID = spark.sparkContext.applicationId
    val startTime = spark.sparkContext.startTime
    logger.info(master)
    log(stage, timer, 0, "END")

    // Reading data...
    timer = System.currentTimeMillis()
    stage = "Data read"
    log(stage, timer, 0, "START")
    val geometryRDD = new SpatialRDD[Geometry]()
    val geometries = spark.read.option("header", "false").option("delimiter", "\t")
      .csv(input).rdd.zipWithUniqueId().map{ row =>
        val geometry = reader.read(row._1.getString(offset)) 
        val userData = (0 until row._1.size).filter(_ != offset).map(i => row._1.getString(i)).mkString("\t")
        geometry.setUserData(userData)
        geometry
      }
    geometryRDD.setRawSpatialRDD(geometries)
    val nGeometries = geometries.count()
    log(stage, timer, nGeometries, "END")

    if(debug){
      geometryRDD.rawSpatialRDD.rdd.foreach(println)
    }

    // Partitioning data...
    timer = clocktime
    stage = "Partitioning data"
    log(stage, timer, 0, "START")
    geometryRDD.analyze()
    geometryRDD.spatialPartitioning(gridType, partitions)
    val grids = geometryRDD.getPartitioner.getGrids.asScala.zipWithIndex.map(g => g._2 -> g._1).toMap
    log(stage, timer, grids.size, "END")

    if(debug) {
      val gridsWKT = grids.values.map(g => s"${envelope2Polygon(g).toText()}\n")
      val f = new java.io.PrintWriter("/tmp/dcelGrid.wkt")
      f.write(gridsWKT.mkString(""))
      f.close()
    }

    // Checking if geoms are simple and valid...
    timer = clocktime
    stage = "Checking validity"
    log(stage, timer, 0, "START")
    val report = geometryRDD.spatialPartitionedRDD.rdd.map{ geom =>
      var feature = "NONE"
      var isValid = false
      var isSimple = false
      if(geom.isInstanceOf[Polygon]){
        val polygon = geom.asInstanceOf[Polygon]
        feature = "Polygon"
        isValid = polygon.isValid()
        isSimple = polygon.isSimple()
      }
      if(geom.isInstanceOf[MultiPolygon]){
        val multipolygon = geom.asInstanceOf[MultiPolygon]
        feature = "Multipolygon"
        isValid = multipolygon.isValid()
        isSimple = multipolygon.isSimple()
      }
      s"${geom.getUserData.toString()}\t${feature}\t${isSimple}\t${isValid}\n"
    }.distinct().collect()
    val f = new java.io.PrintWriter("/tmp/report.txt")
    f.write(report.mkString(""))
    f.close()
    log(stage, timer, report.size, "END")

    // Closing session...
    timer = clocktime
    stage = "Session closed"
    log(stage, timer, 0, "START")
    spark.close()
    log(stage, timer, 0, "END")
  }
}

class GeometryCheckerConf(args: Seq[String]) extends ScallopConf(args) {
  val input:      ScallopOption[String]  = opt[String]  (required = true)
  val offset:     ScallopOption[Int]     = opt[Int]     (default = Some(0))
  val host:       ScallopOption[String]  = opt[String]  (default = Some("169.235.27.138"))
  val port:       ScallopOption[String]  = opt[String]  (default = Some("7077"))
  val cores:      ScallopOption[Int]     = opt[Int]     (default = Some(4))
  val executors:  ScallopOption[Int]     = opt[Int]     (default = Some(3))
  val grid:       ScallopOption[String]  = opt[String]  (default = Some("QUADTREE"))
  val index:      ScallopOption[String]  = opt[String]  (default = Some("QUADTREE"))
  val partitions: ScallopOption[Int]     = opt[Int]     (default = Some(512))
  val local:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val debug:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))

  verify()
}

