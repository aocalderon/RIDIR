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
import com.vividsolutions.jts.geom.{Geometry, Envelope, Coordinate,  Polygon, LinearRing, LineString}
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence
import com.vividsolutions.jts.io.WKTReader
import org.geotools.geometry.jts.GeometryClipper
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object DCEL2{
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val geofactory: GeometryFactory = new GeometryFactory();
  private val reader = new WKTReader(geofactory)
  private val precision: Double = 0.001
  private val startTime: Long = 0L

  case class Segment(face_id: Long, segment_id: Int, x1: Double, y1: Double, x2: Double, y2: Double){
    def toWKT: String = s"${segment_id}\tLINESTRING ($x1 $y1 , $x2 $y2)\t${face_id}"
  }

  def LineString2Segment(line: LineString, segment_id: Int, face_id: Long): Segment = {
    val start = line.getStartPoint
    val end = line.getEndPoint
    Segment(face_id, segment_id, start.getX, start.getY, end.getX, end.getY)
  }

  def Segment2LineString(segment: Segment): LineString = {
    val start = new Coordinate(segment.x1, segment.y1)
    val end = new Coordinate(segment.x2, segment.y2)
    val linestring = geofactory.createLineString(List(start, end).toArray)
    linestring.setUserData(s"${segment.face_id}\t${segment.segment_id}")
    linestring
  }

  def clocktime = System.currentTimeMillis()

  def log(msg: String, timer: Long, n: Long, status: String): Unit ={
    logger.info("DCEL|%6.2f|%-50s|%6.2f|%6d|%s".format((clocktime-startTime)/1000.0, msg, (clocktime-timer)/1000.0, n, status))
  }

  def saveLineString(segments: Dataset[Segment], filename: String): Unit = {
    val f = new java.io.PrintWriter(filename)
    val wkt = segments.rdd.map(_.toWKT).collect.mkString("\n")
    f.write(wkt)
    f.close()
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
    val params: DCELConf = new DCELConf(args)
    val cores = params.cores()
    val executors = params.executors()
    val input = params.input()
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
      .appName("DCEL")
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
    stage = "Getting segments"
    log(stage, timer, 0, "START")
    polygonRDD.analyze()
    polygonRDD.spatialPartitioning(gridType, partitions)
    val segmentsRDD = new SpatialRDD[LineString]()
    val grids = polygonRDD.getPartitioner.getGrids.asScala.zipWithIndex.map(g => g._2 -> g._1).toMap

    grids.map(g => s"${g._1}\t${envelope2Polygon(g._2).toText()}").foreach(println)

    logger.info("Polygons")
    val segments = polygonRDD.spatialPartitionedRDD.rdd.mapPartitionsWithIndex{ (index, polygons) =>
      var results = List.empty[(Segment, Int)]
      if(index < grids.size){
        val clipper = new GeometryClipper(grids(index))
        results = polygons.flatMap{ to_clip =>
          // If false there is no guarantee the polygons returned will be valid according to JTS rules
          // (but should still be good enough to be used for pure rendering).
          val face_id = to_clip.getUserData.toString().toLong
          val geoms = clipper.clip(to_clip, true)
          var polys = new ListBuffer[(Segment, Int)]()
          for(i <- 0 until geoms.getNumGeometries){
            val geom = geoms.getGeometryN(i)
            if(geom.getGeometryType == "Polygon" && !geom.isEmpty()){
              val coords = geom.asInstanceOf[Polygon].getExteriorRing.getCoordinateSequence.toCoordinateArray().toList.zipWithIndex
              coords.zip(coords.tail).map{ pair =>
                val segment_id = pair._1._2
                polys += ((Segment(face_id, segment_id, pair._1._1.x, pair._1._1.y, pair._2._1.x, pair._2._1.y), i))
              }
            }
          }
          polys.toList
        }.toList
      }
      results.toIterator
    }

    segmentsRDD.setRawSpatialRDD(segments.map(s => Segment2LineString(s._1)))
    val nSegments = segments.count()
    log(stage, timer, nSegments, "END")

    //if(debug) { saveLineString(segments, "/tmp/segments.wkt") }

    // Getting pointers...
    timer = clocktime
    stage = "Getting pointers"
    log(stage, timer, 0, "START")
    segments.toDF("segment", "id").groupBy($"id").agg(collect_set($"segment")).rdd.map{ row =>
      val segments = row.getList[Segment](1).asScala
      val first = segments.head
      val coords = List(new Coordinate(first.x1, first.y1)) ++ segments.map(s => new Coordinate(s.x1, s.y1))
      val poly = geofactory.createPolygon(coords.toArray)

      s"${first.face_id}|${poly.toText()}"
    }.collect.foreach(println)
    log(stage, timer, 0, "END")

    // Closing session...
    timer = System.currentTimeMillis()
    stage = "Session closed"
    log(stage, timer, 0, "START")
    spark.close()
    log(stage, timer, 0, "END")
  }  
}
