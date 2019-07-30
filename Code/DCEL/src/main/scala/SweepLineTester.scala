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
import com.vividsolutions.jts.operation.buffer.BufferParameters
import com.vividsolutions.jts.geom.GeometryFactory
import com.vividsolutions.jts.geom.{Geometry, Envelope, Coordinate,  Polygon, LinearRing, LineString}
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence
import com.vividsolutions.jts.geomgraph.index.{SimpleMCSweepLineIntersector, SegmentIntersector}
import com.vividsolutions.jts.algorithm.RobustLineIntersector
import com.vividsolutions.jts.io.WKTReader
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object SweepLineTester{
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val geofactory: GeometryFactory = new GeometryFactory();
  private val reader = new WKTReader(geofactory)
  private val precision: Double = 0.001
  private val startTime: Long = 0L

  def clocktime = System.currentTimeMillis()

  def log(msg: String, timer: Long, n: Long, status: String): Unit ={
    logger.info("DCEL|%6.2f|%-50s|%6.2f|%6d|%s".format((clocktime-startTime)/1000.0, msg, (clocktime-timer)/1000.0, n, status))
  }

  def main(args: Array[String]) = {
    val params = new SweepLineTesterConf(args)
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
      .appName("SweepLine")
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
    val half_edgesRDD = new SpatialRDD[LineString]()
    val half_edges = spark.read.option("header", "false").option("delimiter", "\t")
      .csv(input).rdd.zipWithUniqueId().map{ row =>
        val half_edges = reader.read(row._1.getString(offset)).asInstanceOf[LineString]
        val userData = (0 until row._1.size).filter(_ != offset).map(i => row._1.getString(i)).mkString("\t")
        half_edges.setUserData(userData)
        half_edges
      }
    half_edgesRDD.setRawSpatialRDD(half_edges)
    val nHalf_edges = half_edges.count()
    log(stage, timer, nHalf_edges, "END")

    // Test sweepline...
    timer = clocktime
    stage = "Test sweepline"
    log(stage, timer, 0, "START")
    val part = params.part()
    val hedges = half_edges.collect()
    hedges.map(hedge => s"${hedge.toText()}\t${hedge.getUserData.toString()}").foreach(println)
    val data = hedges.map{ hedge =>
      val arr = hedge.getUserData.toString().split("\t")
      val tag = arr(0).head
      val id = arr(0).tail
      val part = arr(1).toInt
      (part, tag, id, hedge)
    }//.filter(_._3 != "*")
    val data1 = data.filter(x => x._1 == part & x._2 == 'A')
    val data2 = data.filter(x => x._1 == part & x._2 == 'B')

    logger.info("Segments")
    data1.map(_._4).foreach(println)
    data2.map(_._4).foreach(println)

    val gedges1 = data1.map{ hedge =>
      val coords = hedge._4.getCoordinates
      val half_edge = Half_edge(Vertex(coords(0).x, coords(0).y), Vertex(coords(1).x, coords(1).y))
      half_edge.tag = s"${hedge._2}"
      half_edge.label = hedge._3
      new GraphEdge(coords, half_edge)
    }.toList.asJava
    val gedges2 = data2.map{ hedge =>
      val coords = hedge._4.getCoordinates
      val half_edge = Half_edge(Vertex(coords(0).x, coords(0).y), Vertex(coords(1).x, coords(1).y))
      half_edge.tag = s"${hedge._2}"
      half_edge.label = hedge._3
      new GraphEdge(coords, half_edge)
    }.toList.asJava
    val sweepline = new SimpleMCSweepLineIntersector()
    val lineIntersector = new RobustLineIntersector()
    val segmentIntersector = new SegmentIntersector(lineIntersector, true, true)
    sweepline.computeIntersections(gedges1, gedges2, segmentIntersector)

    logger.info("Intersections")
    val g1 = gedges1.asScala.flatMap{ e =>
      e.getVerticesAndIncidents
    }
    val g2 = gedges2.asScala.flatMap{ e =>
      e.getVerticesAndIncidents
    }
    val g = g1.union(g2)

    g.map(g => s"${g._1.toWKT2}\t${g._2.toWKT}").foreach(println)
    
    val h = g.map(g => (g._2, List(g._1))).groupBy(_._1).mapValues{ seq =>
      seq.reduce{ (a, b) => (a._1, a._2 ++ b._2) }._2
    }.toList

    h.map(m => s"${m._1.toWKT}\t${m._2.map(a => a.toWKT2).mkString(", ")}").foreach(println)

    val j = h.map{ row =>
      val hedges = row._2
      val edges = hedges.map(h => (h, s"${h.tag}${h.label}")).groupBy(_._1).mapValues{ seq =>
        seq.reduce{ (a, b) => (a._1, a._2 ++ b._2) }._2
      }
      (row._1, edges)
    }.toList.map{ j =>
      val vertex = j._1
      val half_edges = j._2.map{ e =>
        val hedge = e._1
        hedge.label = e._2
        hedge
      }.toList
      (vertex, half_edges)
    }

    j.map(j => s"${j._1.toWKT}\t${j._2.map(_.toWKT2).mkString("| ")}").foreach(println)

    log(stage, timer, 0, "END")

    // Closing session...
    timer = System.currentTimeMillis()
    stage = "Session closed"
    log(stage, timer, 0, "START")
    spark.close()
    log(stage, timer, 0, "END")    
  }
}

class SweepLineTesterConf(args: Seq[String]) extends ScallopConf(args) {
  val input:      ScallopOption[String]  = opt[String]  (required = true)
  val offset:     ScallopOption[Int]     = opt[Int]     (default = Some(0))
  val host:       ScallopOption[String]  = opt[String]  (default = Some("169.235.27.138"))
  val port:       ScallopOption[String]  = opt[String]  (default = Some("7077"))
  val cores:      ScallopOption[Int]     = opt[Int]     (default = Some(4))
  val executors:  ScallopOption[Int]     = opt[Int]     (default = Some(3))
  val grid:       ScallopOption[String]  = opt[String]  (default = Some("QUADTREE"))
  val index:      ScallopOption[String]  = opt[String]  (default = Some("QUADTREE"))
  val partitions: ScallopOption[Int]     = opt[Int]     (default = Some(512))
  val part:       ScallopOption[Int]     = opt[Int]     (default = Some(0))
  val local:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val debug:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))

  verify()  
}