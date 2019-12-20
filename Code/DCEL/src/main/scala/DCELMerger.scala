import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import com.vividsolutions.jts.algorithm.CGAlgorithms
import com.vividsolutions.jts.geom.{Coordinate, Envelope, Geometry, LineString, LinearRing, Point, Polygon}
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.SparkContext
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
  private val geofactory: GeometryFactory = new GeometryFactory();
  private val precision: Double = 1 / model.getScale

  case class Settings(spark: SparkSession, params: DCELMergerConf, appId: String, startTime: Long)

  def timer[A](msg: String)(code: => A)(implicit settings: Settings): A = {
    val start = clocktime
    val resutl = code
    val end = clocktime
    val executors = settings.params.executors()
    val cores = settings.params.cores()
    val tick = (end - settings.startTime) / 1000.0
    val time = (end - start) / 1000.0

    val log = f"DCELMerger|${settings.appId}|$executors%2d|$cores%2d|$msg%-30s|$time%6.2f"
    logger.info(log)

    code
  }

  def debug[A](code: => A)(implicit settings: Settings): Unit = {
    if(settings.params.debug()){
      code
    }
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
    (polygons, nPolygons)
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

  /***
   * The main function...
   **/
  def main(args: Array[String]) = {
    val params     = new DCELMergerConf(args)
    val cores      = params.cores()
    val executors  = params.executors()
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
    val spark = SparkSession.builder()
        .config("spark.default.parallelism", 3 * cores * executors)
        .config("spark.serializer",classOf[KryoSerializer].getName)
        .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
        .config("spark.scheduler.mode", "FAIR")
        .config("spark.cores.max", cores * executors)
        .config("spark.executor.cores", cores)
        .appName("DCELMerger")
        .getOrCreate()
    import spark.implicits._
    val appID = spark.sparkContext.applicationId.split("-").last
    val startTime = spark.sparkContext.startTime
    implicit val settings = Settings(spark, params, appID, startTime)
    logger.info("Starting session... Done!")

    // Reading polygons...
    val (polygonsA, nPolygonsA) = timer{"Reading polygons A"}{
      readPolygonsA
    }
    val (polygonsB, nPolygonsB) = timer{"Reading polygons B"}{
      readPolygonsB
    }

    debug{
      logger.info(s"Polygons in A: $nPolygonsA")
      logger.info(s"Polygons in B: $nPolygonsB")
    }

    // Partitioning edges...
    val (edgesRDD, nEdgesRDD, cells) = timer{"Partitioning edges"}{
      val edgesA = getEdges(polygonsA)
      val edgesB = getEdges(polygonsB)
      val edges = edgesA.union(edgesB).cache
      val (edgesRDD, boundary) = setSpatialRDD(edges)
      val quadtree = getQuadTree(edges, boundary)
      val cells = quadtree.getLeafZones.asScala.map(cell => (cell.partitionId.toInt -> cell)).toMap
      val partitioner = new QuadTreePartitioner(quadtree)
      edgesRDD.spatialPartitioning(partitioner)
      edgesRDD.spatialPartitionedRDD.rdd.cache
      val nEdgesRDD = edgesRDD.spatialPartitionedRDD.rdd.count()
      (edgesRDD, nEdgesRDD, cells)
    }

    debug{
      logger.info(s"Total number of edges in raw: ${edgesRDD.rawSpatialRDD.count()}")
      logger.info(s"Total number of edges in spatial: ${edgesRDD.spatialPartitionedRDD.count()}")
    }

    // Extracting segments...
    val (segments, nSegments) = timer{"Extracting segments"}{
      val segments = edgesRDD.spatialPartitionedRDD.rdd.mapPartitionsWithIndex{ case (index, edgesIt) =>
        val edges = edgesIt.toVector
        val gA = edges.filter(isA).map(edge2graphedge).toList
        val gB = edges.filter(isB).map(edge2graphedge).toList

        val g = SweepLine.getGraphEdgeIntersections(gA, gB).flatMap{_.getGraphEdges}

        val cell = envelope2Polygon(cells.get(index).get.getEnvelope)
        val gcell = linestring2graphedge(cell.getExteriorRing, "*")

        SweepLine.getGraphEdgeIntersections(g, gcell).flatMap{_.getLineStrings}
          .filter(line => line.coveredBy(cell))
          .toIterator
      }.cache
      val nSegments = segments.count()
      (segments, nSegments)
    }

    debug{ logger.info(s"Segments: $nSegments") }

    // Getting half edges...
    val (half_edges, nHalf_edges) = timer{"Getting half edges"}{
      getHalf_edges(segments)
    }
    
    debug{ logger.info(s"Half edges: $nHalf_edges") }

    /*
    // Getting local DCEL's...
    val (dcel, nDcel) = timer{"Getting local DCEL's"}{
      getLocalDCELs(half_edges)
    }
     

    debug{
      logger.info(s"Partitions on DCEL: ${dcel.getNumPartitions}")

      val nvertices = dcel.map{_.nVertices}.sum()
      logger.info(s"Number of vertices: ${nvertices}")
      val nhedges = dcel.map{_.nHalf_edges}.sum()
      logger.info(s"Number of half edges: ${nhedges}")
      val nfaces = dcel.map{_.nFaces}.sum()
      logger.info(s"Number of faces: ${nfaces}")
    }
     */
    if(params.save()){
      save{"/tmp/edgesCells.wkt"}{
        cells.values.map{ cell =>
          s"${cell.partitionId}\t${envelope2Polygon(cell.getEnvelope)}\n"
        }.toVector
      }

      save{"/tmp/edgesSegments.wkt"}{
        segments.map{ segment =>
          s"${segment.toText()}\n"
        }.collect()
      }

      save{"/tmp/edgesHedges.wkt"}{
        half_edges.map{ hedge =>
          s"${hedge.toWKT2}\n"
        }.collect()
      }
/*
      save{"/tmp/edgesVertices.wkt"}{
        dcel.flatMap{ dcel =>
          dcel.vertices.map{ vertex =>
            s"${vertex.toWKT}\n"
          }
        }.collect()
      }

      save{"/tmp/edgesHedges.wkt"}{
        dcel.flatMap{ dcel =>
          dcel.half_edges.map{ hedge =>
            s"${hedge.id}\t${hedge.toWKT2}\n"
          }
        }.collect()
      }

      save{"/tmp/edgesFaces.wkt"}{
        dcel.flatMap{ dcel =>
          dcel.faces.map{ face =>
            s"${face.id}\t${face.cell}\t${face.getGeometry._1.toText()}\t${face.getGeometry._2}\n"
          }
        }.collect()
      }
 */      
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

  val cores:       ScallopOption[Int]     = opt[Int]     (default = Some(4))
  val executors:   ScallopOption[Int]     = opt[Int]     (default = Some(3))
  val grid:        ScallopOption[String]  = opt[String]  (default = Some("KDBTREE"))
  val index:       ScallopOption[String]  = opt[String]  (default = Some("QUADTREE"))
  val partitions:  ScallopOption[Int]     = opt[Int]     (default = Some(512))
  val fraction:    ScallopOption[Double]  = opt[Double]  (default = Some(0.25))
  val maxentries:  ScallopOption[Int]     = opt[Int]     (default = Some(500))
  val nlevels:     ScallopOption[Int]     = opt[Int]     (default = Some(6))
  val local:       ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val debug:       ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val save:        ScallopOption[Boolean] = opt[Boolean] (default = Some(false))

  verify()
}

