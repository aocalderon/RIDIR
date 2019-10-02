import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

import com.vividsolutions.jts.algorithm.CGAlgorithms
import com.vividsolutions.jts.geom.{Coordinate, Envelope, Geometry, GeometryFactory, LineString, LinearRing, Point, Polygon}
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{Dataset, SparkSession}
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.geotools.geometry.jts.GeometryClipper
import org.rogach.scallop._
import org.slf4j.{Logger, LoggerFactory}

object DCELMerger{
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

  def saveWKT(rdd: RDD[String], filename: String): Unit = {
    val f = new java.io.PrintWriter(filename)
    val wkt = rdd.collect.mkString("")
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
    var timer = clocktime
    var vertexList = edges.flatMap(e => List(e.v1, e.v2)).toSet
    logger.info(s"Vertex set... ${(clocktime - timer) / 1000.0}")

    // Step 2.  Edge set creation with left and right labels...
    timer = clocktime
    val r = edges.cross(edges.map(e => Edge(e.v2, e.v1, "", e.id)))
      .filter(e => e._1.id < e._2.id)
      .filter(e => e._1.v1 == e._2.v1 && e._1.v2 == e._2.v2)
    val toUpdate = r.map{ p =>
      p._1.r = p._2.id
      p._1
    }
    val toDelete = r.map(_._1) ++ r.map(e => Edge(e._2.v2, e._2.v1, "", e._2.id))
    val edgesSet = edges.filterNot(toDelete.toSet).union(toUpdate.toList)
    logger.info(s"Edge set... ${(clocktime - timer) / 1000.0}")

    // Step 3. Half-edge list creation with twins and vertices assignments...
    timer = clocktime
    edgesSet.foreach{ edge =>
      val h1 = Half_edge(edge.v1, edge.v2)
      h1.label = edge.l
      h1.id = edge.l
      h1.tag = tag
      val h2 = Half_edge(edge.v2, edge.v1)
      h2.label = edge.r
      h2.id = edge.r
      h2.tag = tag

      h1.twin = h2
      h2.twin = h1

      vertexList.find(_.equals(edge.v2)).get.half_edges += h1
      vertexList.find(_.equals(edge.v1)).get.half_edges += h2

      half_edgeList += h2
      half_edgeList += h1
    }
    logger.info(s"Half-edge set... ${(clocktime - timer) / 1000.0}")

    // Step 4. Identification of next and prev half-edges...
    timer = clocktime
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
    logger.info(s"Next and prev set... ${(clocktime - timer) / 1000.0}")

    // Step 5. Face assignment...
    timer = clocktime
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
    logger.info(s"Face set... ${(clocktime - timer) / 1000.0}")

    LocalDCEL(half_edgeList.toList, faces.toList, vertexList.toList, edgesSet)
  }

  def readPolygons(spark: SparkSession, input: String, offset: Int): SpatialRDD[Geometry] = {
    val polygonRDD = new SpatialRDD[Geometry]()
    val polygons = spark.read.option("header", "false").option("delimiter", "\t")
      .csv(input).rdd.zipWithUniqueId().map{ row =>
        val polygon = new WKTReader(geofactory).read(row._1.getString(offset))
        val userData = (0 until row._1.size).filter(_ != offset).map(i => row._1.getString(i)).mkString("\t")
        polygon.setUserData(userData)
        polygon
      }
    polygonRDD.setRawSpatialRDD(polygons)
    polygonRDD.rawSpatialRDD.cache()
    polygonRDD
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

  def clipPolygons(polygons: SpatialRDD[Geometry], grids: Map[Int, Envelope]): RDD[Polygon] = {
    val clippedPolygonsRDD = polygons.spatialPartitionedRDD.rdd.mapPartitionsWithIndex{ (index, polygons) =>
      var polys = List.empty[Polygon]
      if(index < grids.size){
        val clipper = new GeometryClipper(grids(index))
        // If false there is no guarantee the polygons returned will be valid according to JTS rules
        // (but should still be good enough to be used for pure rendering).
        var ps = new ListBuffer[Polygon]()
        polygons.foreach { to_clip =>
          val label = to_clip.getUserData.toString
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
    clippedPolygonsRDD
  }

  def buildDCEL(clippedPolygons: RDD[Polygon], tag: String = ""): RDD[LocalDCEL]= {
    val dcel = clippedPolygons.mapPartitionsWithIndex{ (i, polygons) =>
      val edges = polygons.flatMap{ p =>
        var edges = ListBuffer[Edge]()
        for(ring <- getRings(p)){
          val coords = ring.toList
          val segments = coords.zip(coords.tail)
          segments.foreach{ segment =>
            // Assuming id value is in first position...
            val id = p.getUserData.toString().split("\t")(0)
            val v1 = Vertex(segment._1.x, segment._1.y)
            val v2 = Vertex(segment._2.x, segment._2.y)
            edges += Edge(v1, v2, p.getUserData.toString(), id)
          }
        }
        edges.toList
      }
      val start = clocktime
      val dcel = buildLocalDCEL(edges.toList, tag)
      val end = clocktime
      dcel.tag = tag
      dcel.id = i
      dcel.nEdges = dcel.edges.size
      dcel.executionTime = end - start
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
    stage = "Polygons A read"
    log(stage, timer, 0, "START")
    val polygonsA = readPolygons(spark, input1, offset1)
    val nPolygonsA = polygonsA.rawSpatialRDD.rdd.count()
    polygonsA.analyze()
    log(stage, timer, nPolygonsA, "END")

    timer = System.currentTimeMillis()
    stage = "Polygons B read"
    log(stage, timer, 0, "START")
    val polygonsB = readPolygons(spark, input2, offset2)
    val nPolygonsB = polygonsB.rawSpatialRDD.rdd.count()
    polygonsB.analyze()
    log(stage, timer, nPolygonsB, "END")

    // Partitioning data...
    timer = clocktime
    stage = "Partitioning polygons"
    log(stage, timer, 0, "START")
    val boundary1 = polygonsA.boundaryEnvelope
    val boundary2 = polygonsB.boundaryEnvelope
    val fullBoundary = envelope2Polygon(boundary1).union(envelope2Polygon(boundary2)).getEnvelopeInternal
    val points = polygonsB.rawSpatialRDD.rdd.flatMap(p => p.getCoordinates.map(geofactory.createPoint))
    val pointsRDD = new SpatialRDD[Point]()
    pointsRDD.setRawSpatialRDD(points)
    pointsRDD.analyze()
    pointsRDD.boundaryEnvelope = fullBoundary
    pointsRDD.spatialPartitioning(gridType, partitions)
    polygonsA.spatialPartitioning(pointsRDD.getPartitioner)
    polygonsB.spatialPartitioning(pointsRDD.getPartitioner)
    val grids = pointsRDD.getPartitioner.getGrids.asScala.zipWithIndex.map(g => g._2 -> g._1).toMap
    log(stage, timer, grids.size, "END")

    if(debug) {
      val gridsWKT =  pointsRDD.partitionTree.getAllZones().asScala.filter(_.partitionId != null)
        .map(z => s"${z.partitionId}\t${envelope2Polygon(z.getEnvelope).toText()}\n")
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
    timer = clocktime
    stage = "Merging DCEL A and B"
    log(stage, timer, 0, "START")
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
    }.mapPartitionsWithIndex{ (i, hedges) =>
      val dcel: MergedDCEL = SweepLine.buildMergedDCEL(hedges.toList, i)
      List(dcel).toIterator
    }.cache()
    val nMergedDCEL = mergedDCEL.count()
    log(stage, timer, nMergedDCEL, "END")

    if(debug){
      val anRDD = mergedDCEL.mapPartitionsWithIndex{ (i, dcels) =>
        dcels.toList.head.edges.map(e => s"${e.toWKT}\n").toIterator
      }.collect()
      val f = new java.io.PrintWriter("/tmp/edges.wkt")
      f.write(anRDD.mkString(""))
      f.close()
      logger.info(s"Saved edges.wkt [${anRDD.size} records]")
    }
    if(debug){
      val anRDD = mergedDCEL.mapPartitionsWithIndex{ (i, dcels) =>
        dcels.toList.head.source.map(e => s"${e.toWKT3}\t${e.tag}${e.label}\n").toIterator
      }.collect()
      val f = new java.io.PrintWriter("/tmp/source.wkt")
      f.write(anRDD.mkString(""))
      f.close()
      logger.info(s"Saved source.wkt [${anRDD.size} records]")
    }
    if(debug){
      val anRDD = mergedDCEL.mapPartitionsWithIndex{ (i, dcels) =>
        dcels.toList.head.vertices.flatMap{ v =>
          v.half_edges.map(h => s"${v.toWKT}\t${h.angle}\t${h.toWKT}\n")
        }.toIterator
      }.collect()
      val f = new java.io.PrintWriter("/tmp/vertices.wkt")
      f.write(anRDD.mkString(""))
      f.close()
      logger.info(s"Saved vertices.wkt [${anRDD.size} records]")
    }
    if(debug){
      val anRDD = mergedDCEL.mapPartitionsWithIndex{ (i, dcels) =>
        val dcel = dcels.toList.head
        dcel.half_edges.map{ h =>
          s"${h.v2.toWKT}\t${h.toWKT}\t${h.angle}\t${dcel.partition}\t${i}\n"
        }.toIterator
      }.collect()
      val f = new java.io.PrintWriter("/tmp/hedges.wkt")
      f.write(anRDD.mkString(""))
      f.close()
      logger.info(s"Saved hedges.wkt [${anRDD.size} records]")
    }
    if(debug){
      val anRDD = mergedDCEL.mapPartitionsWithIndex{ (i, dcels) =>
        dcels.toList.head.faces.map{ f =>
          s"${f.toWKT}\t${f.tag}\t${f.nHalf_edges}\n"
        }.toIterator
      }.collect()
      val f = new java.io.PrintWriter("/tmp/faces.wkt")
      f.write(anRDD.mkString(""))
      f.close()
      logger.info(s"Saved faces.wkt [${anRDD.size} records]")
    }

    def mergePolygons(a: String, b:String): String = {
      val reader = new WKTReader(geofactory)
      val pa = reader.read(a)
      val pb = reader.read(b)
      pa.union(pb).toText()
    }

    // Running overlay operations...
    timer = clocktime
    stage = "Union"
    log(stage, timer, 0, "START")
    val union = mergedDCEL.mapPartitions{ dcels =>
      dcels.next().union()
        .map(f => (f.tag, f.toWKT))
        .toIterator
    }.reduceByKey{ mergePolygons }.map(f => s"${f._1}\t${f._2}\n").cache()
    log(stage, timer, union.count(), "END")
    saveWKT(union, "/tmp/union.wkt")

    timer = clocktime
    stage = "Intersection"
    log(stage, timer, 0, "START")
    val intersection = mergedDCEL.mapPartitions{ dcels =>
      dcels.next().intersection()
        .map(f => (f.tag, f.toWKT))
        .toIterator
    }.reduceByKey{ mergePolygons }.map(f => s"${f._1}\t${f._2}\n").cache()
    log(stage, timer, intersection.count(), "END")
    saveWKT(intersection, "/tmp/intersection.wkt")

    timer = clocktime
    stage = "Symmetric difference"
    log(stage, timer, 0, "START")
    val symmetric = mergedDCEL.mapPartitions{ dcels =>
      dcels.next().symmetricDifference()
        .map(f => (f.tag, f.toWKT()))
        .toIterator
    }.reduceByKey{ mergePolygons }.map(f => s"${f._1}\t${f._2}\n").cache()
    log(stage, timer, symmetric.count(), "END")
    saveWKT(symmetric, "/tmp/symmetric.wkt")

    timer = clocktime
    stage = "Difference A"
    log(stage, timer, 0, "START")
    val differenceA = mergedDCEL.mapPartitions{ dcels =>
      dcels.next().differenceA()
        .map(f => (f.tag, f.toWKT()))
        .toIterator
    }.reduceByKey{ mergePolygons }.map(f => s"${f._1}\t${f._2}\n").cache()
    log(stage, timer, differenceA.count(), "END")
    saveWKT(differenceA, "/tmp/differenceA.wkt")

    timer = clocktime
    stage = "Difference B"
    log(stage, timer, 0, "START")
    val differenceB = mergedDCEL.mapPartitions{ dcels =>
      dcels.next().differenceB()
        .map(f => (f.tag, f.toWKT()))
        .toIterator
    }.reduceByKey{ mergePolygons }.map(f => s"${f._1}\t${f._2}\n").cache()
    log(stage, timer, differenceB.count(), "END")
    saveWKT(differenceB, "/tmp/differenceB.wkt")

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
  val levels:     ScallopOption[Int]     = opt[Int]     (default = Some(128))
  val entries:    ScallopOption[Int]     = opt[Int]     (default = Some(50))
  val fraction:   ScallopOption[Double]  = opt[Double]  (default = Some(0.25))
  val local:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val debug:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))

  verify()
}
