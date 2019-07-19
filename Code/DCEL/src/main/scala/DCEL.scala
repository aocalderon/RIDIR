/***
 * Implementation based on https://github.com/anglyan/dcel/blob/master/dcel/dcel.py 
 ***/

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
import com.vividsolutions.jts.algorithm.RobustCGAlgorithms
import com.vividsolutions.jts.io.WKTReader
import org.geotools.geometry.jts.GeometryClipper
import scala.collection.JavaConverters._
import scala.collection.mutable.{ListBuffer, TreeSet, ArrayBuffer, HashSet}

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

  def saveWKT(edges: RDD[Half_edge], filename: String): Unit = {
    val f = new java.io.PrintWriter(filename)
    val wkt = edges.map(_.toWKT).collect.mkString("\n")
    f.write(wkt)
    f.close()
  }

  def saveVertices(vertices: RDD[Vertex], filename: String): Unit = {
    val f = new java.io.PrintWriter(filename)
    val wkt = vertices.map(_.toWKT).collect.mkString("\n")
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

  def buildLocalDCEL(vertices: List[Vertex], edges: List[Edge2]): List[String] = {
    var half_edgeList = new ArrayBuffer[Half_edge]()
    var faceList = new ArrayBuffer[Face]()

    // Step 1. Vertex list creation
    var vertexList = vertices.toVector

    // Step 2. Half-edge list creation.  Assignment of twins and vertices
    edges.foreach{ edge =>
      val h1 = Half_edge(edge.v1, edge.v2)
      val h2 = Half_edge(edge.v2, edge.v1)

      h1.twin = h2
      h2.twin = h1

      vertexList.find(_.equals(edge.v2)).get.half_edges += h1
      vertexList.find(_.equals(edge.v1)).get.half_edges += h2

      half_edgeList += h2
      half_edgeList += h1
    }

    // Step 3. Identification of next and prev half-edges
    vertexList = vertexList.map{ vertex =>
      val sortedIncidents = vertex.half_edges.toList.sortBy(_.angle)(Ordering[Double].reverse)
      val size = sortedIncidents.size

      if(size < 2){
        logger.error("Badly formed dcel: less than two hedges in vertex")
      }
      
      for(i <- 0 until (size - 1)){
        var current = sortedIncidents(i)
        var next = sortedIncidents(i + 1)
        current = half_edgeList.find(_.equals(current)).get
        next = half_edgeList.find(_.equals(next)).get
        current.next   = next.twin
        next.twin.prev = current
      }
      var current = sortedIncidents(size - 1)
      var next = sortedIncidents(0)
      current = half_edgeList.find(_.equals(current)).get
      next = half_edgeList.find(_.equals(next)).get
      current.next   = next.twin
      next.twin.prev = current

      // sortedIncidents(size - 1).next = sortedIncidents(0).twin
      // sortedIncidents(0).twin.prev = sortedIncidents(size - 1)      

      val sorted = new HashSet[Half_edge]()
      sorted ++= sortedIncidents
      vertex.half_edges = sorted
      vertex.hedges_size = size
      
      vertex
    }

    // Step 4. Face assignment
    var nf = 0
    var temp_half_edgeList = new ArrayBuffer[Half_edge]()
    temp_half_edgeList ++= half_edgeList
    for(temp_hedge <- temp_half_edgeList){
      val hedge = half_edgeList.find(_.equals(temp_hedge)).get
      if(hedge.face == null){
        val f = Face(nf)
        nf = nf + 1
        f.outerComponent = hedge
        f.outerComponent.face = f
        var h = hedge.next
        while(h != f.outerComponent){
          half_edgeList.find(_.equals(h)).get.face = f
          h = h.next
        }
        faceList += f
      }
    }

    vertexList.flatMap(v => v.half_edges.map{ h =>
      s"${v.toWKT}\t${h.toWKT}"
    }).toList
    /*vertexList.flatMap(v => v.half_edges.map{ h =>
      s"${h.face.toWKT()}\t${h.face.area()}\t${h.face.perimeter()}"
    }).toList.distinct*/
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

    // Partitioning data...
    timer = clocktime
    stage = "Partitioning data"
    log(stage, timer, 0, "START")
    polygonRDD.analyze()
    polygonRDD.spatialPartitioning(gridType, partitions)
    //val segmentsRDD = new SpatialRDD[LineString]()
    val grids = polygonRDD.getPartitioner.getGrids.asScala.zipWithIndex.map(g => g._2 -> g._1).toMap
    if(debug) { grids.map(g => s"${g._1}\t${envelope2Polygon(g._2).toText()}").foreach(println) }
    log(stage, timer, grids.size, "END")

    // Extracting clipped polygons...
    timer = clocktime
    stage = "Extracting clipped polygons"
    log(stage, timer, 0, "START")
    val clippedPolygonsRDD = polygonRDD.spatialPartitionedRDD.rdd.mapPartitionsWithIndex{ (index, polygons) =>
      var polys = List.empty[Polygon]
      if(index < grids.size){
        val clipper = new GeometryClipper(grids(index))
        // If false there is no guarantee the polygons returned will be valid according to JTS rules
        // (but should still be good enough to be used for pure rendering).
        var ps = new ListBuffer[Polygon]()
        polygons.foreach { to_clip =>
          val label = to_clip.getUserData
          val geoms = clipper.clip(to_clip, true)
          for(i <- 0 until geoms.getNumGeometries){
            val geom = geoms.getGeometryN(i)
            if(geom.getGeometryType == "Polygon" && !geom.isEmpty()){
              val p = geom.asInstanceOf[Polygon]
              p.setUserData(label)
              ps += p
            }
          }
        }
        polys = ps.toList
      }
      polys.toIterator
    }.cache()
    val nClippedPolygonsRDD = clippedPolygonsRDD.count()
    log(stage, timer, nClippedPolygonsRDD, "END")

    val verticesRDD = clippedPolygonsRDD.mapPartitionsWithIndex{ (i, polygons) =>
      var vertices = new scala.collection.mutable.HashSet[Vertex]()
      val edges    = polygons.flatMap{ p =>
        val coords = p.getExteriorRing.getCoordinateSequence.toCoordinateArray().toList
        val segments = coords.zip(coords.tail)
        segments.map{ segment =>
          val v1 = Vertex(segment._1.x, segment._1.y)
          val v2 = Vertex(segment._2.x, segment._2.y)
          vertices += v1
          vertices += v2
          if(v1 < v2){
            Edge2(v1, v2)
          } else {
            Edge2(v2, v1)
          }
        }
      }.toSet
      val test = buildLocalDCEL(vertices.toList, edges.toList)
      List((i, vertices.toList, edges.toList, test)).toIterator
    }

    if(debug){
      val clippedWKT = clippedPolygonsRDD.mapPartitionsWithIndex{ (i, polygons) =>
        polygons.map(p => s"${p.toText()}\t${i}\n")
      }.collect().mkString("")
      var f = new java.io.PrintWriter("/tmp/clipped.wkt")
      f.write(clippedWKT)
      f.close()

      val verticesWKT = verticesRDD.mapPartitionsWithIndex{ (i, params) =>
        params.flatMap(data => data._2.map(v => s"${v.toWKT}\t${i}\n"))
      }.collect()
      f = new java.io.PrintWriter("/tmp/vertices.wkt")
      f.write(verticesWKT.mkString(""))
      f.close()
      logger.info(s"Saved vertices.wkt [${verticesWKT.size} records]")

      val edgesWKT = verticesRDD.mapPartitionsWithIndex{ (i, params) =>
        params.flatMap(data => data._3.map(e => s"${e.toWKT}\t${i}\n"))
      }.collect()
      f = new java.io.PrintWriter("/tmp/edges.wkt")
      f.write(edgesWKT.mkString(""))
      f.close()
      logger.info(s"Saved edges.wkt [${edgesWKT.size} records]")

      val incidentsWKT = verticesRDD.mapPartitionsWithIndex{ (i, params) =>
        params.flatMap(data => data._4.map(j => s"${j}\t${i}\n"))
      }.collect()
      f = new java.io.PrintWriter("/tmp/incidents.wkt")
      f.write(incidentsWKT.mkString(""))
      f.close()
      logger.info(s"Saved incidents.wkt [${incidentsWKT.size} records]")

      val incidents = verticesRDD.flatMap(_._4).mapPartitionsWithIndex{ (i,line) =>
        line.map{ l =>
          val arr = l.split("\t")
          val vertex = arr(1)
          val incident = arr(2)

          (vertex, incident, i)
        }
      }.toDF("vertex", "incident", "part")

      incidents.show(false)

      logger.info(s"Count of incidents: ${incidents.count()}")

      val uniqueVertices = incidents.select($"part", $"vertex").distinct()

      logger.info(s"Count of unique vertices: ${uniqueVertices.count()}")

      val countIncidents = incidents.groupBy($"incident").count()

      logger.info(s"Count of incidents: ${countIncidents.count()}")

      countIncidents.orderBy($"count").show(countIncidents.count().toInt, truncate = false)
      
    }

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
  val grid:       ScallopOption[String]  = opt[String]  (default = Some("QUADTREE"))
  val index:      ScallopOption[String]  = opt[String]  (default = Some("QUADTREE"))
  val partitions: ScallopOption[Int]     = opt[Int]     (default = Some(512))
  val local:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val debug:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))

  verify()
}

