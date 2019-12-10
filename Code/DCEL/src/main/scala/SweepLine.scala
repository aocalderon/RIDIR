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

object SweepLine{
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val geofactory: GeometryFactory = new GeometryFactory();
  private val reader = new WKTReader(geofactory)
  private val precision: Double = 0.001
  private val startTime: Long = 0L

  def clocktime = System.currentTimeMillis()

  def log(msg: String, timer: Long, n: Long, status: String): Unit ={
    logger.info("DCEL|%6.2f|%-50s|%6.2f|%6d|%s".format((clocktime-startTime)/1000.0, msg, (clocktime-timer)/1000.0, n, status))
  }

  def computeIntersections(edgesA: List[GraphEdge], edgesB: List[GraphEdge]): List[Half_edge] = {
    val gedges1 = edgesA.asJava
    val gedges2 = edgesB.asJava
    val sweepline = new SimpleMCSweepLineIntersector()
    val lineIntersector = new RobustLineIntersector()
    val segmentIntersector = new SegmentIntersector(lineIntersector, true, true)
    sweepline.computeIntersections(gedges1, gedges2, segmentIntersector)

    val g1 = gedges1.asScala.flatMap(_.getHalf_edges)
    val g2 = gedges2.asScala.flatMap(_.getHalf_edges)
    
    g1.union(g2).toList.groupBy(g => g).map{ case (k, v) =>
      val label = v.map(x => s"${x.tag}${x.label}").mkString(" ")
      k.label = label
      k
    }.toList
  }

  def getGraphEdgeIntersections(edgesA: List[GraphEdge], edgesB: List[GraphEdge]): List[GraphEdge] = {
    val gedges1 = edgesA.asJava
    val gedges2 = edgesB.asJava
    val sweepline = new SimpleMCSweepLineIntersector()
    val lineIntersector = new RobustLineIntersector()
    val segmentIntersector = new SegmentIntersector(lineIntersector, true, true)
    sweepline.computeIntersections(gedges1, gedges2, segmentIntersector)

    val g1 = gedges1.asScala
    val g2 = gedges2.asScala
    
    g1.union(g2).toList
  }

  def buildMergedDCEL(hedges: List[Half_edge], p: Int = -1, debug: Boolean = false): MergedDCEL = {
    var half_edgeList = new ArrayBuffer[Half_edge]()
    var faceList = new ArrayBuffer[Face]()

    // Step 1. Vertex set creation...
    val vertexList = hedges.map(h => Vertex(h.v2.x, h.v2.y) ).distinct

    if(debug){
      logger.info(s"VertexList size: ${vertexList.size}")
      logger.info(s"\n${vertexList.map(v => s"${v.toWKT}\n").mkString("")}")
    }
    // Step 2.  Edge set creation with left and right labels...
    val edges1 = hedges.map(e => Edge(e.v1, e.v2) -> s"${e.label}").toMap
    val edges2 = hedges.map(e => Edge(e.v2, e.v1) -> s"${e.label}").toMap
    val keys = (edges1.keySet ++ edges2.keySet).filter(e => e.v1 < e.v2)
    val edgesSet = keys.map{ e => (e, s"${edges1.getOrElse(e, "*")}<br>${edges2.getOrElse(e, "*")}") }
      .map{ x =>
        val edge = x._1
        edge.label = x._2
        edge
      }

    // Step 3. Half-edge list creation with twins and vertices assignments...
    edgesSet.foreach{ edge =>
      val h1 = Half_edge(edge.v1, edge.v2)
      h1.label = edge.left
      val h2 = Half_edge(edge.v2, edge.v1)
      h2.label = edge.right

      h1.twin = h2
      h2.twin = h1

      vertexList.find(_.equals(edge.v2)).get.half_edges += h1
      vertexList.find(_.equals(edge.v1)).get.half_edges += h2

      half_edgeList += h2
      half_edgeList += h1
    }
    
    // Step 4. Identification of next and prev half-edges...
    val vtest = Vertex(2694179.9939951915, 264715.9088478392)
    vertexList.map{ vertex =>
      val sortedIncidents = vertex.half_edges.toList.sortBy(_.angle)(Ordering[Double].reverse)
      val size = sortedIncidents.size

      if(size < 2){
        logger.error("Badly formed dcel: less than two hedges in vertex")
      }

      for(i <- 0 until (size - 1)){
        val current = sortedIncidents(i)
        val next    = sortedIncidents(i + 1)
        val current2 = half_edgeList.find{ h => h.equals(current) }.get
        val next2    = half_edgeList.find{ h => h.equals(next) }.get
        current2.next   = next2.twin
        next2.twin.prev = current2
      }
      val current = sortedIncidents(size - 1)
      val next    = sortedIncidents(0)
      val current2 = half_edgeList.find{ h => h.equals(current) }.get
      val next2    = half_edgeList.find{ h => h.equals(next) }.get
      current2.next   = next2.twin
      next2.twin.prev = current2
    }

    // Step 5. Face assignment...
    var temp_half_edgeList = new ArrayBuffer[Half_edge]()
    temp_half_edgeList ++= half_edgeList
    for(temp_hedge <- temp_half_edgeList){
      val hedge = half_edgeList.find(_.equals(temp_hedge)).get
      val tag = new ArrayBuffer[String]()
      var count = 0
      if(!hedge.label.contains("*")){
        tag += hedge.label
        count = 1
      }
      if(!hedge.visited){
        val f = Face(hedge.label)
        f.outerComponent = hedge
        f.outerComponent.face = f
        var h1 = hedge.next
        while(h1 != f.outerComponent){
          val h2 = half_edgeList.find(_.equals(h1)).get
          h2.face = f
          h2.visited = true
          if(!h2.label.contains("*")){
            tag += h2.label
            count += 1
          }
          h1 = h1.next
        }
        if(f.area() < 0) { f.exterior = true }
        f.tag = tag.toList.distinct.mkString(" ").split(" ").distinct.sorted.mkString(" ")
        faceList += f
      }
    }
    MergedDCEL(half_edgeList.toList, faceList.toList, vertexList.toList, p, edgesSet, hedges)
  }

  def prepareEdges(hedges: Array[LineString]): List[Half_edge] = {
    hedges.map(hedge => s"${hedge.toText()}\t${hedge.getUserData.toString()}").foreach(println)
    val data = hedges.map{ hedge =>
      val arr = hedge.getUserData.toString().split("\t")
      val tag = arr(0).head
      val id = arr(0).tail
      val part = arr(1).toInt
      (part, tag, id, hedge)
    }//.filter(_._3 != "*")
    val data1 = data.filter(x => /*x._1 == part & */x._2 == 'A')
    val data2 = data.filter(x => /*x._1 == part & */x._2 == 'B')

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
      e.getHalf_edges
    }
    val g2 = gedges2.asScala.flatMap{ e =>
      e.getHalf_edges
    }
    g1.union(g2).toList
  }

  def main(args: Array[String]) = {
    val params = new SweepLineConf(args)
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

    val g = prepareEdges(hedges)

    logger.info("Merged DCEL")
    val dcel = buildMergedDCEL(g)
    val verticesWKT = dcel.vertices.map(v => s"${v.toWKT}\n").mkString("")
    var f = new java.io.PrintWriter("/tmp/vertices.wkt")
    f.write(verticesWKT)
    f.close()
    val half_edgesWKT = dcel.half_edges.map(h => s"${h.toWKT}\t${h.label}\n").mkString("")
    f = new java.io.PrintWriter("/tmp/half_edges.wkt")
    f.write(half_edgesWKT)
    f.close
    val facesWKT = dcel.faces.map(f => s"${f.toWKT}\t${f.tag}\t${f.area()}\n").mkString("")
    f = new java.io.PrintWriter("/tmp/faces.wkt")
    f.write(facesWKT)
    f.close

    log(stage, timer, 0, "END")

    // Closing session...
    timer = System.currentTimeMillis()
    stage = "Session closed"
    log(stage, timer, 0, "START")
    spark.close()
    log(stage, timer, 0, "END")    
  }
}

class SweepLineConf(args: Seq[String]) extends ScallopConf(args) {
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
