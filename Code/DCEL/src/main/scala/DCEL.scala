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

object DCEL{
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

  def buildLocalDCEL(edges: List[Edge]): LocalDCEL = {
    var half_edgeList = new ArrayBuffer[Half_edge]()
    var faceList = new ArrayBuffer[Face]()

    // Step 1. Vertex set creation...
    var vertexList = edges.flatMap(e => List(e.v1, e.v2)).toSet

    // Step 2.  Edge set creation with left and right labels...
    val r = edges.cross(edges.map(e => Edge(e.v2, e.v1, "", e.id)))
      .filter(e => e._1.id < e._2.id)
      .filter(e => e._1.v1 == e._2.v1 && e._1.v2 == e._2.v2)
    val toUpdate = r.map{ p =>
      p._1.r = p._2.id
      p._1
    }
    val toDelete = r.map(_._1) ++ r.map(e => Edge(e._2.v2, e._2.v1, "", e._2.id))
    val edgesSet = edges.filterNot(toDelete.toSet).union(toUpdate.toList)

    // Step 3. Half-edge list creation with twins and vertices assignments...
    edgesSet.foreach{ edge =>
      val h1 = Half_edge(edge.v1, edge.v2)
      h1.label = edge.l
      h1.id = edge.l
      val h2 = Half_edge(edge.v2, edge.v1)
      h2.label = edge.r
      h2.id = edge.r

      h1.twin = h2
      h2.twin = h1

      vertexList.find(_.equals(edge.v2)).get.half_edges += h1
      vertexList.find(_.equals(edge.v1)).get.half_edges += h2

      half_edgeList += h2
      half_edgeList += h1
    }

    // Step 4. Identification of next and prev half-edges...
    vertexList.map{ vertex =>
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
    }

    // Step 5. Face assignment...
    var temp_half_edgeList = new ArrayBuffer[Half_edge]()
    temp_half_edgeList ++= half_edgeList
    for(temp_hedge <- temp_half_edgeList){
      val hedge = half_edgeList.find(_.equals(temp_hedge)).get
      if(hedge.face == null){
        val f = Face(hedge.label)
        f.outerComponent = hedge
        f.outerComponent.face = f
        f.id = hedge.id
        var h = hedge
        do{
          half_edgeList.find(_.equals(h)).get.face = f
          h = h.next
        }while(h != f.outerComponent)
        faceList += f
      }
    }

    val faces = faceList.groupBy(_.id).map{ case (id, faces) =>
      val f = faces.toList.sortBy(_.faceArea())(Ordering[Double].reverse)
      val head = f.head
      val tail = f.tail
      head.innerComponent = tail

      head
    }

    LocalDCEL(half_edgeList.toList, faces.toList, vertexList.toList, edgesSet)
  }

  /***
   * The main function...
   **/
  def main(args: Array[String]) = {
    val params: DCELConf = new DCELConf(args)
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
      //.config("spark.cores.max", cores * executors)
      //.config("spark.executor.cores", cores)
      //.master(master)
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
    val polygonRDD = new SpatialRDD[Geometry]()
    val polygons = spark.read.textFile(input).rdd.zipWithUniqueId().map{ case (line, i) =>
      val arr = line.split("\t")
      val userData = (0 until arr.size).filter(_ != offset).map(i => arr(i))
      logger.info(s"Parsing ${userData(0)}")
      val wkt = arr(offset)
      val coords = Array(new Coordinate(30, 10), new Coordinate(40, 40), new Coordinate(20, 40), new Coordinate(10, 20), new Coordinate(30, 10))
      var polygon = geofactory.createPolygon(geofactory.createLinearRing(coords)).asInstanceOf[Geometry]
      try{
        polygon = reader.read(wkt)
        polygon.setUserData(userData.mkString("\t"))
        polygon
      } catch {
        case e: com.vividsolutions.jts.io.ParseException =>
          logger.info(s"Error in ${userData(0)} ${e.getMessage()} ${wkt}")
        case e: java.lang.NullPointerException =>
          logger.info(s"Error in ${userData(0)} ${e.getMessage()} ${wkt}")
      }
      polygon
    }
    polygonRDD.setRawSpatialRDD(polygons)
    val nPolygons = polygons.count()
    log(stage, timer, nPolygons, "END")

    if(debug){
      //polygonRDD.rawSpatialRDD.rdd.foreach(println)
    }

    // Partitioning data...
    timer = clocktime
    stage = "Partitioning data"
    log(stage, timer, 0, "START")
    polygonRDD.analyze()
    polygonRDD.spatialPartitioning(gridType, partitions)
    val grids = polygonRDD.getPartitioner.getGrids.asScala.zipWithIndex.map(g => g._2 -> g._1).toMap
    log(stage, timer, grids.size, "END")

    if(debug) {
      val gridsWKT = grids.values.map(g => s"${envelope2Polygon(g).toText()}\n")
      val f = new java.io.PrintWriter("/tmp/dcelGrid.wkt")
      f.write(gridsWKT.mkString(""))
      f.close()
    }

    // Extracting clipped polygons...
    timer = clocktime
    stage = "Extracting clipped polygons"
    log(stage, timer, 0, "START")
    val clippedPolygonsRDD = polygonRDD.spatialPartitionedRDD.rdd.mapPartitionsWithIndex{ (index, polygons) =>
      var polys = List.empty[Polygon]
      if(index < grids.size){
        val clipper = new GeometryClipper(grids(index))
        var ps = new ListBuffer[Polygon]()
        polygons.foreach { to_clip =>
          val label = to_clip.getUserData.toString
          // If false there is no guarantee the polygons returned will be valid according to JTS rules
          // (but should still be good enough to be used for pure rendering).
          val geoms = clipper.clipSafe(to_clip, true, 0.001)
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

    // Building Local DCEL...
    timer = clocktime
    stage = "Building local DCEL"
    log(stage, timer, 0, "START")
    val dcelRDD = clippedPolygonsRDD.mapPartitionsWithIndex{ (i, polygons) =>
      val edges = polygons.flatMap{ p =>
        var edges = ListBuffer[Edge]()
        for(ring <- getRings(p)){
          val coords = ring.toList
          val segments = coords.zip(coords.tail)
          segments.foreach{ segment =>
            // Assuming id value in in first position...
            val id = p.getUserData.toString().split("\t")(0)
            val v1 = Vertex(segment._1.x, segment._1.y)
            val v2 = Vertex(segment._2.x, segment._2.y)
            edges += Edge(v1, v2, p.getUserData.toString(), id)
          }
        }
        edges.toList
      }
      val dcel = buildLocalDCEL(edges.toList)
      dcel.id = i
      List(dcel).toIterator
    }.cache()
    val nDcelRDD = dcelRDD.count()
    log(stage, timer, nDcelRDD, "END")

    if(debug){
/*
      val clippedWKT = clippedPolygonsRDD.mapPartitionsWithIndex{ (i, polygons) =>
        polygons.map(p => s"${p.toText()}\t${i}\n")
      }.collect()
      var f = new java.io.PrintWriter("/tmp/clipped.wkt")
      f.write(clippedWKT.mkString(""))
      f.close()
      logger.info(s"Saved clipped.wkt [${clippedWKT.size} records]")

      val verticesWKT = dcelRDD.mapPartitionsWithIndex{ (i, dcel) =>
        dcel.flatMap(d => d.vertices.map(f => s"${f.toWKT}\t${i}\n"))
      }.collect()
      f = new java.io.PrintWriter("/tmp/vertices.wkt")
      f.write(verticesWKT.mkString(""))
      f.close()
      logger.info(s"Saved vertices.wkt [${verticesWKT.size} records]")
      val hedgesWKT = dcelRDD.mapPartitionsWithIndex{ (i, dcel) =>
        dcel.flatMap(d => d.half_edges.map(f => s"${f.toWKT}\t${f.origen.toWKT}\t${f.angle}\t${i}\n"))
      }.collect()
      f = new java.io.PrintWriter("/tmp/hedges.wkt")
      f.write(hedgesWKT.mkString(""))
      f.close()
 
      logger.info(s"Saved hedges.wkt [${hedgesWKT.size} records]")
 */
      val facesWKT = dcelRDD.mapPartitionsWithIndex{ (i, dcel) =>
        dcel.flatMap(d => d.faces.map(f => s"${f.toWKT2}\t${f.area()}\t${f.perimeter()}\t${i}\n"))
      }.collect()
      val f = new java.io.PrintWriter("/tmp/faces.wkt")
      f.write(facesWKT.mkString(""))
      f.close()
      logger.info(s"Saved faces.wkt [${facesWKT.size} records]")
/*
      val edgesWKT = dcelRDD.mapPartitionsWithIndex{ (i, dcel) =>
        dcel.flatMap(d => d.edges.map(f => s"${f.toWKT}\t${f.toString}\t${i}\n"))
      }.collect()
      f = new java.io.PrintWriter("/tmp/edges.wkt")
      f.write(edgesWKT.mkString(""))
      f.close()
      logger.info(s"Saved edges.wkt [${edgesWKT.size} records]")
*/
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

