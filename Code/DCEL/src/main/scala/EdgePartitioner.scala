import org.slf4j.{LoggerFactory, Logger}
import org.rogach.scallop._
import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.spatialPartitioning.quadtree._
import org.datasyslab.geospark.spatialRDD.{SpatialRDD, PolygonRDD, LineStringRDD}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import com.vividsolutions.jts.geom.GeometryFactory
import com.vividsolutions.jts.geom.{Geometry, Envelope, Coordinate,  Polygon, LinearRing, LineString, MultiPolygon, Point}
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence
import com.vividsolutions.jts.algorithm.{RobustCGAlgorithms, CGAlgorithms}
import com.vividsolutions.jts.io.WKTReader
import org.geotools.geometry.jts.GeometryClipper
import scala.collection.JavaConverters._
import scala.collection.mutable.{ListBuffer, TreeSet, ArrayBuffer, HashSet}

object EdgePartitioner{
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val geofactory: GeometryFactory = new GeometryFactory();
  private val reader = new WKTReader(geofactory)
  private val precision: Double = 0.001
  private val startTime: Long = 0L

  implicit class Crossable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]) = for { x <- xs; y <- ys } yield (x, y)
  }
  
  def clocktime = System.currentTimeMillis()

  def log(msg: String, timer: Long, n: Long, status: String): Unit ={
    logger.info("EP|%6.2f|%-50s|%6.2f|%6d|%s".format((clocktime-startTime)/1000.0, msg, (clocktime-timer)/1000.0, n, status))
  }

  def envelope2Polygon(e: Envelope): Polygon = {
    val minX = e.getMinX()
    val minY = e.getMinY()
    val maxX = e.getMaxX()
    val maxY = e.getMaxY()
    val p1 = new Coordinate(minX, minY)
    val p2 = new Coordinate(maxX, minY)
    val p3 = new Coordinate(maxX, maxY)
    val p4 = new Coordinate(minX, maxY)
    val coordArraySeq = new CoordinateArraySequence( Array(p1,p2,p3,p4,p1), 2)
    val ring = new LinearRing(coordArraySeq, geofactory)
    new Polygon(ring, null, geofactory)
  }

  def getRings(polygon: Polygon): List[Array[Coordinate]] = {
    var exteriorCoords = polygon.getExteriorRing.getCoordinateSequence.toCoordinateArray()
    if(!CGAlgorithms.isCCW(exteriorCoords)) { exteriorCoords = exteriorCoords.reverse }
    val outerRing = List(exteriorCoords)

    val nInteriorRings = polygon.getNumInteriorRing
    val innerRings = (0 until nInteriorRings).map{ i => 
      var interiorCoords = polygon.getInteriorRingN(i).getCoordinateSequence.toCoordinateArray()
      if(CGAlgorithms.isCCW(interiorCoords)) { interiorCoords = interiorCoords.reverse }
      interiorCoords
    }.toList

    outerRing ++ innerRings
  }

  def getHalf_edges(polygon: Polygon): List[LineString] = {
    val polygon_id = polygon.getUserData.toString().split("\t")(0)
    val rings = getRings(polygon).zipWithIndex
    rings.flatMap{ ring =>
      val ring_id = ring._2
      ring._1.zip(ring._1.tail).zipWithIndex.map{ pair =>
        val order = pair._2
        val coord1 = pair._1._1
        val coord2 = pair._1._2
        val coords = new CoordinateArraySequence(Array(coord1, coord2))

        val line = new LineString(coords, geofactory)
        line.setUserData(s"${polygon_id}\t${ring_id}\t${order}")

        line
      }
    }
  }

  /***
   * The main function...
   **/
  def main(args: Array[String]) = {
    val params = new EdgePartitionerConf(args)
    val cores = params.cores()
    val executors = params.executors()
    val input = params.input()
    val offset = params.offset()
    val ppartitions = params.ppartitions()
    val epartitions = params.epartitions()
    val debug = params.debug()
    val master = params.local() match {
      case true  => s"local[${cores}]"
      case false => s"spark://${params.host()}:${params.port()}"
    }
    val gridType = params.grid() match {
      case "EQUALGRID" => GridType.EQUALGRID
      case "QUADTREE"  => GridType.QUADTREE
      case "KDBTREE"   => GridType.KDBTREE
    }

    // Starting session...
    var timer = clocktime
    var stage = "Starting session"
    log(stage, timer, 0, "START")
    val spark = SparkSession.builder()
      .config("spark.default.parallelism", 3 * 120)
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .config("spark.scheduler.mode", "FAIR")
      //.config("spark.cores.max", cores * executors)
      //.config("spark.executor.cores", cores)
      //.master(master)
      .appName("EdgePartitioner")
      .getOrCreate()
    import spark.implicits._
    val appID = spark.sparkContext.applicationId
    val startTime = spark.sparkContext.startTime
    log(stage, timer, 0, "END")

    // Reading data...
    timer = System.currentTimeMillis()
    stage = "Reading data"
    log(stage, timer, 0, "START")
    val polygonRDD = new SpatialRDD[Geometry]()
    val polygons = spark.read.textFile(input).rdd.zipWithUniqueId().map{ case (line, i) =>
      val arr = line.split("\t")
      val userData = List(s"$i") ++ (0 until arr.size).filter(_ != offset).map(i => arr(i)) 
      val wkt = arr(offset)
      val polygon = new WKTReader(geofactory).read(wkt)
      polygon.setUserData(userData.mkString("\t"))
      polygon
    }.cache
    polygonRDD.setRawSpatialRDD(polygons)
    val nPolygons = polygons.count()
    log(stage, timer, nPolygons, "END")

    timer = clocktime
    stage = "Partitioning polygons"
    log(stage, timer, 0, "START")
    polygonRDD.analyze()
    polygonRDD.spatialPartitioning(GridType.QUADTREE, ppartitions)
    val nPpartitions = polygonRDD.spatialPartitionedRDD.rdd.getNumPartitions
    log(stage, timer, nPpartitions, "END")

    timer = clocktime
    stage = "Extracting edges"
    log(stage, timer, 0, "START")
    val edges = polygonRDD.spatialPartitionedRDD.rdd
      .map(_.asInstanceOf[Polygon])
      .flatMap(getHalf_edges).cache
    val nEdges = edges.count()
    log(stage, timer, nEdges, "END")

    timer = System.currentTimeMillis()
    stage = "Partitioning edges"
    log(stage, timer, 0, "START")
    val edgesRDD = new SpatialRDD[LineString]()
    edgesRDD.setRawSpatialRDD(edges)
    edgesRDD.analyze()
    //edgesRDD.spatialPartitioning(GridType.QUADTREE, epartitions)
    val boundary = new QuadRectangle(edgesRDD.boundaryEnvelope)
    val quadtree = new StandardQuadTree[LineString](boundary, 0, params.eentries(), params.elevels())
    for(edge <- edges.sample(false, params.esample(), 42).collect()) { 
      quadtree.insert(new QuadRectangle(edge.getEnvelopeInternal), edge)
    }
    quadtree.assignPartitionIds()
    val EdgePartitioner = new QuadTreePartitioner(quadtree)
    edgesRDD.spatialPartitioning(EdgePartitioner)
    val nEpartitions = edgesRDD.spatialPartitionedRDD.rdd.getNumPartitions
    log(stage, timer, nEpartitions, "END")

    if(debug){
      var WKT = edgesRDD.spatialPartitionedRDD.rdd.mapPartitionsWithIndex{ case (i, edges) =>
        edges.map{ edge =>
          s"${edge.toText()}\t${edge.getUserData}\t${i}\n"
        }
      }.collect()
      var filename = "/tmp/edgesRDD.wkt"
      var f = new java.io.PrintWriter(filename)
      f.write(WKT.mkString(""))
      f.close
      logger.info(s"${filename} saved [${WKT.size}] records")

      WKT = quadtree.getLeafZones.asScala.toArray.map(z => s"${z.partitionId}\t${envelope2Polygon(z.getEnvelope)}\n")
      filename = "/tmp/edgesCells.wkt"
      f = new java.io.PrintWriter(filename)
      f.write(WKT.mkString(""))
      f.close
      logger.info(s"${filename} saved [${WKT.size}] records")
    }

    timer = System.currentTimeMillis()
    stage = "Clossing session"
    log(stage, timer, 0, "START")
    spark.close()
    log(stage, timer, 0, "END")
  }  
}

class EdgePartitionerConf(args: Seq[String]) extends ScallopConf(args) {
  val input:       ScallopOption[String]  = opt[String]  (required = true)
  val offset:      ScallopOption[Int]     = opt[Int]     (default = Some(0))
  val host:        ScallopOption[String]  = opt[String]  (default = Some("169.235.27.138"))
  val port:        ScallopOption[String]  = opt[String]  (default = Some("7077"))
  val cores:       ScallopOption[Int]     = opt[Int]     (default = Some(4))
  val executors:   ScallopOption[Int]     = opt[Int]     (default = Some(3))
  val grid:        ScallopOption[String]  = opt[String]  (default = Some("KDBTREE"))
  val index:       ScallopOption[String]  = opt[String]  (default = Some("QUADTREE"))
  val ppartitions: ScallopOption[Int]     = opt[Int]     (default = Some(512))
  val epartitions: ScallopOption[Int]     = opt[Int]     (default = Some(512))
  val esample:     ScallopOption[Double]  = opt[Double]  (default = Some(0.25))
  val eentries:    ScallopOption[Int]     = opt[Int]     (default = Some(500))
  val elevels:     ScallopOption[Int]     = opt[Int]     (default = Some(6))
  val local:       ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val debug:       ScallopOption[Boolean] = opt[Boolean] (default = Some(false))

  verify()
}

