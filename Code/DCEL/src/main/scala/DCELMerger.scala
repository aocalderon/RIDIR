import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import com.vividsolutions.jts.algorithm.CGAlgorithms
import com.vividsolutions.jts.geom.{Coordinate, Envelope, Geometry, LineString, LinearRing, Point, Polygon}
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{Dataset, SparkSession}
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.spatialPartitioning.quadtree.{QuadTreePartitioner, StandardQuadTree, QuadRectangle}
import org.datasyslab.geospark.spatialPartitioning.{KDBTree, KDBTreePartitioner}
import org.geotools.geometry.jts.GeometryClipper
import org.rogach.scallop._
import org.slf4j.{Logger, LoggerFactory}
import DCELBuilder._

object DCELMerger{
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val model: PrecisionModel = new PrecisionModel(1000)
  private val geofactory: GeometryFactory = new GeometryFactory(model);
  private val precision: Double = 1 / model.getScale

  case class Settings(spark: SparkSession, params: DCELMergerConf, conf: SparkConf, startTime: Long)

  def timer[A](msg: String)(code: => A)(implicit settings: Settings): A = {
    val start = clocktime
    val result = code
    val end = clocktime
    val (cores, executors, appId) = if(settings.params.local()){
      val cores = java.lang.Runtime.getRuntime().availableProcessors()
      val appId = settings.conf.get("spark.app.id")
      (cores, 1, appId)
    } else {
      val cores = settings.conf.get("spark.executor.instances").toInt
      val executors = settings.conf.get("spark.executor.cores").toInt
      val appId = settings.conf.get("spark.app.id").takeRight(4)
      (cores, executors, appId)
    }
    val tick = (end - settings.startTime) / 1000.0
    val time = (end - start) / 1000.0
    val partitions = settings.params.partitions()
    val log = f"DCELMerger|$appId|$executors%2d|$cores%2d|$partitions%5d|$msg%-30s|$time%6.2f"
    logger.info(log)

    result
  }

  def debug[A](code: => A)(implicit settings: Settings): Unit = {
    if(settings.params.debug()){
      code
    }
  }

  def log(msg: String)(implicit spark: SparkSession): Unit = {
    val appId = spark.sparkContext.applicationId.takeRight(4)
    logger.info(f"INFO|$appId|$msg")
  }

  def readPolygonsA(implicit settings: Settings): (RDD[Polygon], Long) = {
    val input  = settings.params.input1()
    val offset = settings.params.offset1()
    val spark = settings.spark
    readPolygons(spark: SparkSession, input: String, offset: Int, "A")
  }

  def readPolygonsB(implicit settings: Settings): (RDD[Polygon], Long) = {
    val input  = settings.params.input2()
    val offset = settings.params.offset2()
    val spark = settings.spark
    readPolygons(spark: SparkSession, input: String, offset: Int, "B")
  }

  def readPolygons(spark: SparkSession, input: String, offset: Int, tag: String):
      (RDD[Polygon], Long) = {
    val polygons = spark.read.textFile(input).rdd.zipWithUniqueId().map{ case (line, i) =>
      val arr = line.split("\t")
      val userData = s"${tag}${i}" +: (0 until arr.size).filter(_ != offset).map(i => arr(i))
      val wkt =  arr(offset).replaceAll("\"", "")
      val polygon = new WKTReader(geofactory).read(wkt)
      polygon.setUserData(userData.mkString("\t"))
      polygon.asInstanceOf[Polygon]
    }.cache
    val nPolygons = polygons.count()
    (polygons.filter(_.getUserData.toString().split("\t")(0) != "A57"), nPolygons)
  }

  def getQuadTree(edges: RDD[LineString], boundary: QuadRectangle)(implicit settings: Settings):
      StandardQuadTree[LineString] = {
    val maxentries = settings.params.maxentries()
    val nlevels = settings.params.nlevels()
    val fraction = settings.params.fraction()
    val quadtree = new StandardQuadTree[LineString](boundary, 0, maxentries, nlevels)
    val edgesSample = edges.sample(false, fraction, 42)
    for(edge <- edgesSample.collect()) {
      quadtree.insert(new QuadRectangle(edge.getEnvelopeInternal), edge)
    }
    quadtree.assignPartitionIds()
    quadtree.assignPartitionLineage()

    quadtree
  }

  def getTag(line: LineString): String = {
    val userData = line.getUserData.toString().split("\t")
    val id = userData(0)
    id.take(1)
  }

  def isA(line: LineString): Boolean = { getTag(line) == "A" }

  def isB(line: LineString): Boolean = { getTag(line) == "B" }

  def isValidPoligon(wkt: String): Boolean = {
    val reader = new WKTReader(geofactory)
    val geom = reader.read(wkt)

    geom.isValid()
  }

  /***
   * The main function...
   **/
  def main(args: Array[String]) = {
    val params     = new DCELMergerConf(args)
    val input1     = params.input1()
    val offset1    = params.offset1()
    val input2     = params.input2()
    val offset2    = params.offset2()
    val partitions = params.partitions()
    val gridType   = params.grid() match {
      case "EQUALTREE" => GridType.EQUALGRID
      case "QUADTREE"  => GridType.QUADTREE
      case "KDBTREE"   => GridType.KDBTREE
    }

    // Starting session...
    logger.info("Starting session...")
    implicit val spark = SparkSession.builder()
        .config("spark.serializer",classOf[KryoSerializer].getName)
        .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
        .config("spark.scheduler.mode", "FAIR")
        .appName("DCELMerger")
        .getOrCreate()
    import spark.implicits._
    val startTime = spark.sparkContext.startTime
    val config = spark.sparkContext.getConf
    if(!params.local()){
      val command = System.getProperty("sun.java.command")
      logger.info(command)
    }
    
    implicit val settings = Settings(spark, params, config, startTime)
    logger.info("Starting session... Done!")

    // Reading polygons...
    val (polygonsA, nPolygonsA) = timer{"Reading polygons A"}{
      readPolygonsA
    }
    val (polygonsB, nPolygonsB) = timer{"Reading polygons B"}{
      readPolygonsB
    }

    debug{
      log(f"Polygons in A|$nPolygonsA")
      log(f"Polygons in B|$nPolygonsB")
    }

    // Partitioning edges...
    val (edgesRDD, nEdgesRDD, cells) = timer{"Partitioning edges"}{
      val edgesA = getEdges(polygonsA)
      val edgesB = getEdges(polygonsB)
      val edges = edgesA.union(edgesB).cache
      val (edgesRDD, cells) = if(params.custom()){
        val (edgesRDD, boundary) = setSpatialRDD(edges)
        val quadtree = getQuadTree(edges, boundary)
        val cells = quadtree.getLeafZones.asScala.map(cell => (cell.partitionId.toInt -> cell)).toMap
        val partitioner = new QuadTreePartitioner(quadtree)
        edgesRDD.spatialPartitioning(partitioner)
        (edgesRDD, cells)
      } else {
        val (edgesRDD, boundary) = setSpatialRDD(edges)
        edgesRDD.spatialPartitioning(GridType.QUADTREE, params.partitions())
        val cells = edgesRDD.partitionTree.getLeafZones.asScala.map(cell => (cell.partitionId.toInt -> cell)).toMap
        (edgesRDD, cells)
      }
      edgesRDD.spatialPartitionedRDD.rdd.cache
      val nEdgesRDD = edgesRDD.spatialPartitionedRDD.rdd.count()
      (edgesRDD, nEdgesRDD, cells)
    }

    debug{
      log(f"Total number of partitions|${cells.size}")
      log(f"Total number of edges in raw|${edgesRDD.rawSpatialRDD.count()}")
      log(f"Total number of edges in spatial|${edgesRDD.spatialPartitionedRDD.count()}")
    }

    // Extracting segments...
    def mergeDuplicates(segments: Seq[LineString]): Seq[LineString] = {
      segments.map{ segment =>
        ( (segment.getStartPoint, segment.getEndPoint), segment)
      }
        .groupBy(_._1).values.map(_.map(_._2))
        .flatMap{ lines =>
          if(lines.size > 1){
              val data0 = lines(0).getUserData.toString().split("\t")
              val data1 = lines(1).getUserData.toString().split("\t")
              val new_id = data0.head + "|" + data1.head
              val new_data = new_id +: data0.tail
              val line = lines.head
              line.setUserData(new_data.mkString("\t"))
              
              List(line)
          } else {
            lines
          }
        }.toList
    }

    val (segments, nSegments) = timer{"Extracting segments"}{
      val segments = edgesRDD.spatialPartitionedRDD.rdd.mapPartitionsWithIndex{ case (index, edgesIt) =>
        val edges = edgesIt.toVector
        val gA = edges.filter(isA).map(edge2graphedge).toList
        val gB = edges.filter(isB).map(edge2graphedge).toList
        val cell = envelope2Polygon(cells.get(index).get.getEnvelope)
        val gCell = linestring2graphedge(cell.getExteriorRing, "*")

        val g = SweepLine.getGraphEdgeIntersections(gA, gB).flatMap{_.getLineStrings}
          .map(edge2graphedge)

        val segs = SweepLine.getGraphEdgeIntersections(g, gCell).flatMap{_.getLineStrings}
          .filter(line => line.coveredBy(cell))
        mergeDuplicates(segs).toIterator
      }.cache
      val nSegments = segments.count()
      (segments, nSegments)
    }

    debug{
      log(f"Segments|$nSegments")
    }

    // Getting half edges...
    def getHalf_edges2(segments: RDD[LineString]): (RDD[Half_edge], Long) = {
      val half_edges = segments.mapPartitionsWithIndex{ case (index, segments) =>
        segments.flatMap{ segment =>
          val arr = segment.getUserData.toString().split("\t")
          val coords = segment.getCoordinates
          val v1 = Vertex(coords(0).x, coords(0).y)
          val v2 = Vertex(coords(1).x, coords(1).y)
          val h1 = Half_edge(v1, v2)
          h1.id = arr(0); h1.ring = arr(1).toInt; h1.order = arr(2).toInt
          h1.label = arr(3)
          val h2 = Half_edge(v2, v1)
          h2.id = "*"
          h1.twin = h2
          h2.twin = h1
          List(h1, h2)
        }
      }.cache
      val nHalf_edges = half_edges.count()
      (half_edges, nHalf_edges)
    }

    val (half_edges, nHalf_edges) = timer{"Getting half edges"}{
      getHalf_edges2(segments)
    }
    
    debug{ log(f"Half edges|$nHalf_edges") }
   
    // Merging DCELs...
    def getVerticesByV2(half_edges: Iterator[Half_edge]): Vector[Vertex] = {
      val vertices = half_edges.map(hedge => (hedge.v2, hedge)).toList
        .groupBy(_._1).toList.map{ v =>
          val vertex = v._1
          vertex.setHalf_edges(v._2.map(_._2))
          vertex
        }.toVector

      vertices.foreach{ vertex =>
        val sortedIncidents = vertex.getHalf_edges()
        val size = sortedIncidents.size

        for(i <- 0 until (size - 1)){
          var current = sortedIncidents(i)
          var next    = sortedIncidents(i + 1)
          current.next   = next.twin
          next.twin.prev = current
        }
        var current = sortedIncidents(size - 1)
        var next    = sortedIncidents(0)
        current.next   = next.twin
        next.twin.prev = current
      }
      vertices
    }
    
    def getHedgesList2(vertices: Vector[Vertex], index: Int): Unit = {
      val hedges = vertices.flatMap(_.getHalf_edges).toVector

      hedges.map { _.toWKT3 }.foreach{ println }
    }

    def preprocess(vertices: Vector[Vertex], index: Int, tag: String = ""):
        Vector[(Face, Vector[Half_edge])] = {
      getHedgesList2(vertices, index)
      getHedgesList(vertices).map{ hedges =>
        val id = hedges.map(_.id).distinct.filter(_ != "*" ).sorted.mkString("|")
        (id, hedges.map{ h => h.id = id; h })
      }
        .map{ case (id, hedges) =>
          val hedge = hedges.head
          val face = Face(id, index)
          face.id = id
          face.tag = tag
          face.outerComponent = hedge
          val new_hedges = hedges.map{ h =>
            h.face = face
            h.tag = tag
            h
          }

          (face, new_hedges)
        }
    }

    def getHedges2(pre: Vector[(Face, Vector[Half_edge])]): Vector[Half_edge] = {
      pre.flatMap(_._2)
    }

    def pruneDuplicateIDs(id: String): String = {
      id.split("\\|").distinct.mkString("|")
    }

    def getFaces2(pre: Vector[(Face, Vector[Half_edge])]): Vector[Face] = {
      pre.map(_._1).filter(_.id != "")
        .groupBy(_.id) // Grouping multi-parts
        .map{ case (id, faces) =>
          val polys = faces.sortBy(_.area).reverse
          val outer = polys.head
          outer.innerComponents = polys.tail.toVector

          outer.id = pruneDuplicateIDs(outer.id)
          outer
        }.toVector
    }

    def doublecheckFaces(faces: Vector[Face], p: Int): Vector[Face] = {
      val f_prime = faces.filter(_.id.split("\\|").size == 1)
      val f_primeA = f_prime.filter(_.id.head == 'A')
      val f_primeB = f_prime.filter(_.id.head == 'B')

      if(f_primeA.isEmpty){
        faces
      } else if(f_primeB.isEmpty){
        faces
      } else {
        val f = for{
          a <- f_primeA
          b <- f_primeB
        } yield {
          logger.info(s"In $p ${a.id} vs ${b.id}")
          if(a.toPolygon().coveredBy(b.toPolygon())){
            a.id = s"${a.id}|${b.id}"
            Vector(a, b)
          } else if(b.toPolygon().coveredBy(b.toPolygon())){
            b.id = s"${a.id}|${b.id}"
            Vector(a, b)
          } else {
            Vector(a,b)
          }
        }
        f.flatten.foreach{i => logger.info(s"In $p ${i.toString}")}

        f.flatten.union(faces.filter(_.id.split("\\|").size == 2))
      }
    }

    val (dcel, nDcel) = timer{"Merging DCELs"}{
      val dcel = half_edges.mapPartitionsWithIndex{ case (index, half_edges) =>

        val hedges = half_edges.toVector
        val vertices = getVerticesByV2(hedges.toIterator)
        val faces = Vector.empty[Face]

        //val pre = preprocess(vertices, index)
        //val hedges = getHedges2(pre)
        //val faces  = doublecheckFaces(getFaces2(pre), index)
        //val faces  = getFaces2(pre)

        Iterator( LDCEL(index, vertices, hedges, faces) )
      }.cache
      val nDcel = dcel.count()
      (dcel, nDcel)
    }
     
    debug{
      log(f"Partitions on DCEL|${dcel.getNumPartitions}")
    }

    def mergePolygons(a: String, b:String): String = {
      val reader = new WKTReader(geofactory)
      val pa = reader.read(a)
      val pb = reader.read(b)
      pa.union(pb).toText()
    }

    def intersection(dcel: LDCEL): Vector[Face] = {
      dcel.faces.filter(_.id.split("\\|").size == 2)
    }
    def union(dcel: LDCEL): Vector[Face] = {
      dcel.faces.filter(_.area() > 0)
    }
    def symmetricDifference(dcel: LDCEL): Vector[Face] = {
      dcel.faces.filter(_.id.split("\\|").size == 1)
        .filter(_.area() > 0)
    }
    def differenceA(dcel: LDCEL): Vector[Face] = {
      symmetricDifference(dcel).filter(_.id.size > 1).filter(_.id.substring(0, 1) == "A")
    }
    def differenceB(dcel: LDCEL): Vector[Face] = {
      symmetricDifference(dcel).filter(_.id.size > 1).filter(_.id.substring(0, 1) == "B")
    }
    
    def overlapOp(dcel: RDD[LDCEL], op: (LDCEL) => Vector[Face], filename: String = "/tmp/overlay.wkt"){
      save{filename}{
        dcel.flatMap{op}
        .map(f => (f.id, f.toPolygon().toText()))
        .reduceByKey(mergePolygons)
          .map{ case(id, wkt) => s"$wkt\t$id\n" }.collect()
      }
    }

    val output_path = "/tmp/edges"
    timer{"Intersection"}{
      overlapOp(dcel, intersection, s"${output_path}OpIntersection.wkt")
    }
    timer{"Union"}{
      overlapOp(dcel, union, s"${output_path}OpUnion.wkt")
    }
    timer{"Symmetric"}{
      overlapOp(dcel, symmetricDifference, s"${output_path}OpSymmetric.wkt")
    }
    timer{"Diff A"}{
      overlapOp(dcel, differenceA, s"${output_path}OpDifferenceA.wkt")
    }
    timer{"Diff B"}{
      overlapOp(dcel, differenceB, s"${output_path}OpDifferenceB.wkt")
    }

    if(params.save()){
      val file_id = ""
      save{s"/tmp/edgesCells${file_id}.wkt"}{
        cells.values.map{ cell =>
          s"${envelope2Polygon(cell.getEnvelope)}\t${cell.partitionId}\n"
        }.toVector
      }

      save{s"/tmp/edgesSegments${file_id}.wkt"}{
        segments.map{ segment =>
          s"${segment.toText()}\t${segment.getUserData.toString()}\n"
        }.collect()
      }

      save{s"/tmp/edgesVertices${file_id}.wkt"}{
        dcel.flatMap{ dcel =>
          dcel.vertices.map{ vertex =>
            vertex.getHalf_edges.map{ hedge =>
              s"${vertex.toWKT}\t${hedge.toWKT3}\n"
            }
          }.flatten
        }.collect()
      }

      save{s"/tmp/edgesHedges${file_id}.wkt"}{
        dcel.flatMap{ dcel =>
          dcel.half_edges.map{ hedge =>
            s"${hedge.toWKT3}\t${hedge.id}\n"
          }
        }.collect()
      }
      save{s"/tmp/edgesFaces${file_id}.wkt"}{
        dcel.flatMap{ dcel =>
          dcel.faces.map{ face =>
            s"${face.getGeometry._1.toText()}\t${face.id}\t${face.cell}\t${face.getGeometry._2}\n"
          }
        }.collect()
      }
    }

    // Closing session...
    logger.info("Closing session...")
    spark.close()
    logger.info("Closing session... Done!")
  }  
}

class DCELMergerConf(args: Seq[String]) extends ScallopConf(args) {
  val input1:      ScallopOption[String]  = opt[String]  (required = true)
  val offset1:     ScallopOption[Int]     = opt[Int]     (default = Some(0))
  val input2:      ScallopOption[String]  = opt[String]  (required = true)
  val offset2:     ScallopOption[Int]     = opt[Int]     (default = Some(0))
  val quote1:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val quote2:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val grid:        ScallopOption[String]  = opt[String]  (default = Some("QUADTREE"))
  val index:       ScallopOption[String]  = opt[String]  (default = Some("QUADTREE"))
  val partitions:  ScallopOption[Int]     = opt[Int]     (default = Some(512))
  val fraction:    ScallopOption[Double]  = opt[Double]  (default = Some(0.25))
  val maxentries:  ScallopOption[Int]     = opt[Int]     (default = Some(500))
  val nlevels:     ScallopOption[Int]     = opt[Int]     (default = Some(6))
  val custom:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val local:       ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val debug:       ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val save:        ScallopOption[Boolean] = opt[Boolean] (default = Some(false))

  verify()
}

