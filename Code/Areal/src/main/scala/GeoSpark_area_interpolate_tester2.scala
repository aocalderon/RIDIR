import org.apache.spark.storage.StorageLevel
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.spatialOperator.JoinQuery
import com.vividsolutions.jts.geom.{GeometryFactory, Geometry}
import com.vividsolutions.jts.io.WKTReader
import org.rogach.scallop._
import org.slf4j.{LoggerFactory, Logger}
import scala.collection.JavaConverters._
import java.io.PrintWriter

object GeoSpark_area_interpolate_tester2{
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val geofactory: GeometryFactory = new GeometryFactory()

  def main(args: Array[String]) = {
    val params     = new GeoSpark_area_interpolate_tester2Conf(args)
    val source     = params.source()
    val target     = params.target()
    val host       = params.host()
    val port       = params.port()
    val partitions = params.partitions()
    val grid       = params.grid()
    val index      = params.index()
    val cores      = params.cores()
    val executors  = params.executors()
    val debug      = params.debug()
    val local      = params.local()
    var master     = ""
    if(local){
      master = s"local[$cores]"
    } else {
      master = s"spark://${host}:${port}"
    }
    val gridType = grid match {
      case "QUADTREE"  => GridType.QUADTREE
      case "RTREE"     => GridType.RTREE
      case "EQUALGRID" => GridType.EQUALGRID
      case "KDBTREE"   => GridType.KDBTREE
      case "HILBERT"   => GridType.HILBERT
      case "VORONOI"   => GridType.VORONOI
    }
    val indexType = index match {
      case "QUADTREE"  => IndexType.QUADTREE
      case "RTREE"     => IndexType.RTREE
    }

    // Starting session...
    val totalTime = clocktime
    var timer = clocktime
    val spark = SparkSession.builder().
      config("spark.serializer",classOf[KryoSerializer].getName).
      master(master).appName("Areal").
      config("spark.cores.max", cores * executors).
      config("spark.executor.cores", cores).
      getOrCreate()
    import spark.implicits._
    val appID = spark.sparkContext.applicationId
    logger.info(s"Session $appID started [${(clocktime - timer) / 1000.0}]")    

    // Reading source...
    timer = clocktime
    val sourceRDD = new SpatialRDD[Geometry]()
    val sourceWKT = spark.read.option("header", "false").option("delimiter", "\t").
      csv(source).rdd.
      map{ s =>
        val geom = new WKTReader(geofactory).read(s.getString(0))
        val geoid = s.getString(1)
        val extensive = s.getString(2)
        var intensive = s.getString(3)
        if(intensive == "null"){ intensive = "0.0"  }

        geom.setUserData(s"S$geoid\t$extensive\t$intensive")
        geom
      }
    sourceRDD.setRawSpatialRDD(sourceWKT)
    val nSourceRDD = sourceRDD.rawSpatialRDD.rdd.count()
    log("Source read", timer, nSourceRDD)

    if(debug){ sourceRDD.rawSpatialRDD.rdd.map(_.getUserData.toString()).toDF().show(false) }

    // Reading target...
    timer = clocktime
    val targetRDD = new SpatialRDD[Geometry]()
    val targetWKT = spark.read.option("header", "false").option("delimiter", "\t").
      csv(target).rdd.
      map{ s =>
        val geom = new WKTReader(geofactory).read(s.getString(0))
        val geoid = s.getString(1)
        geom.setUserData(s"T$geoid")
        geom
      }
    targetRDD.setRawSpatialRDD(targetWKT)
    val nTargetRDD = targetRDD.rawSpatialRDD.rdd.count()
    log("Target read", timer, nTargetRDD)

    if(debug){ targetRDD.rawSpatialRDD.rdd.map(_.getUserData.toString()).toDF().show(false) }

    // Calling area_table method...
    Areal.debug = debug
    Areal.partitions = partitions
    val extensive = List("extensive")
    val intensive = List("intensive")
    val estimates = Areal.area_interpolate2(spark, sourceRDD, targetRDD, extensive, intensive)

    val rextensive = estimates._1.toDF("tid", "extensive").orderBy("extensive")
    rextensive.show(false)

    val rintensive = estimates._2.toDF("tid", "intensive").orderBy("intensive")
    rintensive.show(false)

    val pw1 = new PrintWriter(params.outextensive())
    pw1.write(rextensive.map(t => s"${t.getString(0)}\t${t.getDouble(1)}\n").collect().mkString(""))
    pw1.close()

    val pw2 = new PrintWriter(params.outintensive())
    pw2.write(rintensive.map(t => s"${t.getString(0)}\t${t.getDouble(1)}\n").collect().mkString(""))
    pw2.close()

    // Closing session...
    timer = clocktime
    val ttime = "%.2f".format((clocktime - totalTime) / 1000.0)
    logger.info(s"Total execution time: $ttime s")    
    val url = s"http://${host}:4040/api/v1/applications/${appID}/executors"
    val r = requests.get(url)
    if(s"${r.statusCode}" == "200"){
      import scala.util.parsing.json._
      val j = JSON.parseFull(r.text).get.asInstanceOf[List[Map[String, Any]]]
      j.filter(_.get("id").get != "driver").foreach{ m =>
        val tid    = m.get("id").get
        val thost  = m.get("hostPort").get
        val trdds  = "%.0f".format(m.get("rddBlocks").get)
        val ttasks = "%.0f".format(m.get("totalTasks").get)
        val tcores = "%.0f".format(m.get("totalCores").get)
        val ttime  = "%.2fs".format(m.get("totalDuration").get.asInstanceOf[Double] / 1000.0)
        val tinput = "%.2fMB".format(m.get("totalInputBytes").get.asInstanceOf[Double] / (1024.0 * 1024))
        logger.info(s"EXECUTORS;$tcores;$tid;$trdds;$ttasks;$ttime;$tinput;$thost;$appID")
      }
    }

    spark.close()
    logger.info(s"Session $appID closed [${(clocktime - timer) / 1000.0}]")    
  }

  def clocktime = System.currentTimeMillis()

  def log(msg: String, timer: Long, n: Long = -1): Unit = {
    if(n == -1)
      logger.info("%-50s|%6.2f".format(msg, (clocktime - timer)/1000.0))
    else
      logger.info("%-50s|%6.2f|%6d".format(msg, (clocktime - timer)/1000.0, n))
  }

  def saveWKT(filename: String, wkt: String): Unit = {
    val pw = new PrintWriter(filename)
    pw.write(wkt)
    pw.close()
  }
}

class GeoSpark_area_interpolate_tester2Conf(args: Seq[String]) extends ScallopConf(args) {
  val source:     ScallopOption[String]  = opt[String]  (required = true)
  val target:     ScallopOption[String]  = opt[String]  (required = true)
  val outextensive:     ScallopOption[String]  = opt[String]  (default = Some("/tmp/geospark_extensive.tsv"))
  val outintensive:     ScallopOption[String]  = opt[String]  (default = Some("/tmp/geospark_intensive.tsv"))
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
