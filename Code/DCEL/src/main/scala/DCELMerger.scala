/***
 * Implementation based on https://github.com/anglyan/dcel/blob/master/dcel/dcel.py 
 * with some updates and additions...
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
import org.datasyslab.geospark.spatialPartitioning.quadtree.{QuadTreePartitioner, StandardQuadTree, QuadRectangle}
import com.vividsolutions.jts.operation.buffer.BufferParameters
import com.vividsolutions.jts.geom.GeometryFactory
import com.vividsolutions.jts.geom.{Geometry, Envelope, Coordinate,  Polygon, LinearRing, LineString}
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence
import com.vividsolutions.jts.algorithm.{RobustCGAlgorithms, CGAlgorithms}
import com.vividsolutions.jts.io.WKTReader
import org.geotools.geometry.jts.GeometryClipper
import scala.collection.JavaConverters._
import scala.collection.mutable.{ListBuffer, TreeSet, ArrayBuffer, HashSet}

object DCELMerger{
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

  def buildLocalDCEL(edges: List[Edge], tag: String = ""): LocalDCEL = {
    var half_edgeList = new ArrayBuffer[Half_edge]()
    var faceList = new ArrayBuffer[Face]()

    // Step 1. Vertex set creation...
    var vertexList = edges.flatMap(e => List(e.v1, e.v2)).toSet

    // Step 2.  Edge set creation with left and right labels...
    val edges1 = edges.map(e => Edge(e.v1, e.v2) -> e.label).toMap
    val edges2 = edges.map(e => Edge(e.v2, e.v1) -> e.label).toMap
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
      h1.tag = tag
      val h2 = Half_edge(edge.v2, edge.v1)
      h2.label = edge.right
      h2.tag = tag

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
        f.tag = tag
        f.outerComponent = hedge
        f.outerComponent.face = f
        var h = hedge.next
        while(h != f.outerComponent){
          half_edgeList.find(_.equals(h)).get.face = f
          h = h.next
        }
        if(f.area() < 0) { f.exterior = true }
        faceList += f
      }
    }

    LocalDCEL(half_edgeList.toList, faceList.toList, vertexList.toList)
  }

  def readPolygons(spark: SparkSession, input: String, offset: Int): SpatialRDD[Polygon] = {
    val polygonRDD = new SpatialRDD[Polygon]()
    val polygons = spark.read.option("header", "false").option("delimiter", "\t")
      .csv(input).rdd.zipWithUniqueId().map{ row =>
        val polygon = reader.read(row._1.getString(offset)).asInstanceOf[Polygon]
        val userData = (0 until row._1.size).filter(_ != offset).map(i => row._1.getString(i)).mkString("\t")
        polygon.setUserData(userData)
        polygon
      }
    polygonRDD.setRawSpatialRDD(polygons)
    polygonRDD.rawSpatialRDD.cache()
    polygonRDD
  }

  def clipPolygons(polygons: SpatialRDD[Polygon], grids: Map[Int, Envelope]): RDD[Polygon] = {
    val clippedPolygonsRDD = polygons.spatialPartitionedRDD.rdd.mapPartitionsWithIndex{ (index, polygons) =>
      var polys = List.empty[Polygon]
      if(index < grids.size){
        val clipper = new GeometryClipper(grids(index))
        // If false there is no guarantee the polygons returned will be valid according to JTS rules
        // (but should still be good enough to be used for pure rendering).
        var ps = new ListBuffer[Polygon]()
        polygons.foreach { to_clip =>
          val label = to_clip.getUserData.toString
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
    clippedPolygonsRDD
  }

  def buildDCEL(clippedPolygons: RDD[Polygon], tag: String = ""): RDD[LocalDCEL]= {
    val dcel = clippedPolygons.mapPartitionsWithIndex{ (i, polygons) =>
      val edges = polygons.flatMap{ p =>
        val ring = p.getExteriorRing.getCoordinateSequence.toCoordinateArray()
        val orientation = CGAlgorithms.isCCW(ring)
        var coords = List.empty[Coordinate]
        if(orientation){
          coords = ring.toList
        } else {
          coords = ring.reverse.toList
        }
        val segments = coords.zip(coords.tail)
        segments.map{ segment =>
          val v1 = Vertex(segment._1.x, segment._1.y)
          val v2 = Vertex(segment._2.x, segment._2.y)
          Edge(v1, v2, p.getUserData.toString())
        }
      }
      val dcel = buildLocalDCEL(edges.toList, tag)
      dcel.id = i
      dcel.tag = tag
      List(dcel).toIterator
    }
    dcel
  }

  /***
   * The main function...
   **/
  def main(args: Array[String]) = {
    val params = new DCELMergerConf(args)
    val cores = params.cores()
    val executors = params.executors()
    val input1 = params.input1()
    val offset1 = params.offset1()
    val input2 = params.input2()
    val offset2 = params.offset2()
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
    stage = "Polygons A read"
    log(stage, timer, 0, "START")
    val polygonsA = readPolygons(spark, input1, offset1)
    val nPolygonsA = polygonsA.rawSpatialRDD.rdd.count()
    polygonsA.analyze()
    log(stage, timer, nPolygonsA, "END")

    polygonsA.rawSpatialRDD.rdd.map(p => p.toText()).foreach(println)

    timer = System.currentTimeMillis()
    stage = "Polygons B read"
    log(stage, timer, 0, "START")
    val polygonsB = readPolygons(spark, input2, offset2)
    val nPolygonsB = polygonsB.rawSpatialRDD.rdd.count()
    polygonsB.analyze()
    log(stage, timer, nPolygonsB, "END")

    polygonsB.rawSpatialRDD.rdd.map(p => p.toText()).foreach(println)

    // Partitioning data...
    timer = clocktime
    stage = "Partitioning polygons"
    log(stage, timer, 0, "START")
    val boundary1 = polygonsA.boundaryEnvelope
    val boundary2 = polygonsB.boundaryEnvelope
    if(debug){ logger.info(s"Testing...") }
    val fullBoundary = envelope2Polygon(boundary1).union(envelope2Polygon(boundary2)).getEnvelopeInternal
    val samplesA = polygonsA.rawSpatialRDD.rdd.sample(false, params.fraction(), 42).map(_.getEnvelopeInternal)
    val samplesB = polygonsB.rawSpatialRDD.rdd.sample(false, params.fraction(), 42).map(_.getEnvelopeInternal)
    val samples = samplesA.union(samplesB)

    samples.collect().map(p => envelope2Polygon(p).toText()).foreach(println)
    if(debug){ logger.info(s"Sample' size: ${samples.count()}") }

    val boundary = new QuadRectangle(fullBoundary)
    val maxLevels = params.levels()
    val maxEntriesPerNode = params.entries()
    val quadtree = new StandardQuadTree[Geometry](boundary, 0, maxEntriesPerNode, maxLevels)
    if(debug){ logger.info(s"Sample' size: ${samples.count()}") }
    for(sample <- samples.collect()){
      quadtree.insert(new QuadRectangle(sample), null)
    }
    quadtree.assignPartitionIds()
    val QTPartitioner = new QuadTreePartitioner(quadtree)
    polygonsA.spatialPartitioning(QTPartitioner)
    polygonsB.spatialPartitioning(polygonsA.getPartitioner)
    val grids = quadtree.getAllZones.asScala.filter(_.partitionId != null)
      .map(r => r.partitionId.toInt -> r.getEnvelope).toMap
    log(stage, timer, grids.size, "END")

    if(debug) {
      val gridsWKT = grids.values.map(g => s"${envelope2Polygon(g).toText()}\n")
      val f = new java.io.PrintWriter("/tmp/dcelGrid.wkt")
      f.write(gridsWKT.mkString(""))
      f.close()
    }

    // Extracting clipped polygons A...
    timer = clocktime
    stage = "Extracting clipped polygons A"
    log(stage, timer, 0, "START")
    val clippedPolygonsA  = clipPolygons(polygonsA, grids)
    val nClippedPolygonsA = clippedPolygonsA.count()
    log(stage, timer, nClippedPolygonsA, "END")

    // Extracting clipped polygons B...
    timer = clocktime
    stage = "Extracting clipped polygons B"
    log(stage, timer, 0, "START")
    val clippedPolygonsB  = clipPolygons(polygonsB, grids)
    val nClippedPolygonsB = clippedPolygonsB.count()
    log(stage, timer, nClippedPolygonsB, "END")

    // Building Local DCEL in A...
    timer = clocktime
    stage = "Building local DCEL in A"
    log(stage, timer, 0, "START")
    val dcelA = buildDCEL(clippedPolygonsA, "A").cache()
    val nDcelA = dcelA.count()
    log(stage, timer, nDcelA, "END")

    // Building Local DCEL in B...
    timer = clocktime
    stage = "Building local DCEL in B"
    log(stage, timer, 0, "START")
    val dcelB = buildDCEL(clippedPolygonsB, "B").cache()
    val nDcelB = dcelB.count()
    log(stage, timer, nDcelB, "END")

    // Merging DCEL A and B...
    val mergedHalf_edges = dcelA.zipPartitions(dcelB, true)((iterA, iterB) => iterA ++ iterB)
      .mapPartitionsWithIndex{ (i, dcels) =>
        dcels.flatMap(_.half_edges)
      }
    if(debug){
      val hedges = mergedHalf_edges.mapPartitionsWithIndex{ (i, hedges) =>
        hedges.map(hedge => s"${hedge.toWKT}\t${i}\n")
      }.collect()
      val f = new java.io.PrintWriter("/tmp/hedges.wkt")
      f.write(hedges.mkString(""))
      f.close()
      logger.info(s"Saved hedges.wkt [${hedges.size} records]")

    }

    val mergedDCEL = dcelA.zipPartitions(dcelB, true){ (iterA, iterB) =>
      val gedgesA = iterA.flatMap(_.half_edges).map{ a =>
        val coord1 = new Coordinate(a.v1.x, a.v1.y)
        val coord2 = new Coordinate(a.v2.x, a.v2.y)
        new GraphEdge(Array(coord1, coord2), a)
      }
      val gedgesB = iterB.flatMap(_.half_edges).map{ b =>
        val coord1 = new Coordinate(b.v1.x, b.v1.y)
        val coord2 = new Coordinate(b.v2.x, b.v2.y)
        new GraphEdge(Array(coord1, coord2), b)
      }
      SweepLine.computeIntersections(gedgesA.toList, gedgesB.toList).toIterator
      //gedgesA ++ gedgesB
    }.mapPartitionsWithIndex{ (i, hedges) =>
      val dcel: MergedDCEL = SweepLine.buildMergedDCEL(hedges.toList)
      dcel.edges.toIterator
    }

    if(debug){
      val facesRDD = mergedDCEL.mapPartitionsWithIndex{ (i, dcels) =>
        //val part = dcels.toList.head
        //val id = part._1
        //val dcel = part._2
        //dcel.faces.map(f => s"${i}\t${id}\t${f.toWKT()}\t${f.tag}\n").toIterator
        //dcels.flatMap(x => x.half_edges.map(h => s"${x.toWKT}\t${h.toWKT2}\t${h.label}\n"))
        dcels.map(e => s"${e.toWKT}\n")
      }.collect()
      val f = new java.io.PrintWriter("/tmp/test1.wkt")
      f.write(facesRDD.mkString(""))
      f.close()
      logger.info(s"Saved faces.wkt [${facesRDD.size} records]")
    }

    // Closing session...
    timer = System.currentTimeMillis()
    stage = "Session closed"
    log(stage, timer, 0, "START")
    spark.close()
    log(stage, timer, 0, "END")
  }  
}

class DCELMergerConf(args: Seq[String]) extends ScallopConf(args) {
  val input1:     ScallopOption[String]  = opt[String]  (required = true)
  val offset1:    ScallopOption[Int]     = opt[Int]     (default = Some(0))
  val input2:     ScallopOption[String]  = opt[String]  (required = true)
  val offset2:    ScallopOption[Int]     = opt[Int]     (default = Some(0))
  val host:       ScallopOption[String]  = opt[String]  (default = Some("169.235.27.138"))
  val port:       ScallopOption[String]  = opt[String]  (default = Some("7077"))
  val cores:      ScallopOption[Int]     = opt[Int]     (default = Some(4))
  val executors:  ScallopOption[Int]     = opt[Int]     (default = Some(3))
  val grid:       ScallopOption[String]  = opt[String]  (default = Some("QUADTREE"))
  val index:      ScallopOption[String]  = opt[String]  (default = Some("QUADTREE"))
  val partitions: ScallopOption[Int]     = opt[Int]     (default = Some(512))
  val levels:     ScallopOption[Int]     = opt[Int]     (default = Some(16))
  val entries:    ScallopOption[Int]     = opt[Int]     (default = Some(64))
  val fraction:   ScallopOption[Double]  = opt[Double]  (default = Some(0.1))
  val local:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val debug:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))

  verify()
}
