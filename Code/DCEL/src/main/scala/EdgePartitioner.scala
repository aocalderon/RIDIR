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
import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel}
import com.vividsolutions.jts.geom.{Geometry, Envelope, Coordinate,  Polygon, LinearRing, LineString, MultiPolygon, Point}
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence
import com.vividsolutions.jts.algorithm.{RobustCGAlgorithms, CGAlgorithms}
import com.vividsolutions.jts.io.WKTReader
import org.geotools.geometry.jts.GeometryClipper
import scala.collection.JavaConverters._

object EdgePartitioner{
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val startTime: Long = 0L
  private val precisionModel: PrecisionModel = new PrecisionModel(1000)
  private val geofactory: GeometryFactory = new GeometryFactory(precisionModel)

  case class Quad(id: Int, nedges: Int, lineage: String, region: Int, wkt: String, siblings: List[Int])
  case class EmptyCell(cell: QuadRectangle, var solve: Boolean){
    var nextCellWithEdgesId: Int = -1
    var referenceCorner: Point = null

    override def toString: String = s"${cell.partitionId}\t${solve}\t${nextCellWithEdgesId}"
  }

  implicit class Crossable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]) = for { x <- xs; y <- ys } yield (x, y)
  }
  
  def clocktime = System.currentTimeMillis()

  def timer[A](msg: String)(code: => A): A = {
    val start = clocktime
    val resutl = code
    val end = clocktime
    logger.info("%-30s|%6.2f".format(msg, (end - start) / 1000.0))
    code
  }

  def debug[A](code: => A)(implicit debugState: Boolean): Unit = {
    if(debugState){
      code
    }
  }

  def save(filename: String)(content: Seq[String]): Unit = {
    val start = clocktime
    val f = new java.io.PrintWriter(filename)
    f.write(content.mkString(""))
    f.close
    val end = clocktime
    val time = "%.2f".format((end - start) / 1000.0)
    logger.info(s"Saved ${filename} in ${time}s [${content.size} records].")
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
    val ring = geofactory.createLinearRing(Array(p1,p2,p3,p4,p1))
    geofactory.createPolygon(ring)
  }

  def getRings(polygon: Polygon): List[Array[Coordinate]] = {
    val ecoords = polygon.getExteriorRing.getCoordinateSequence.toCoordinateArray()
    val outerRing = if(!CGAlgorithms.isCCW(ecoords)) { ecoords.reverse } else { ecoords }
    
    val nInteriorRings = polygon.getNumInteriorRing
    val innerRings = (0 until nInteriorRings).map{ i => 
      val icoords = polygon.getInteriorRingN(i).getCoordinateSequence.toCoordinateArray()
      if(CGAlgorithms.isCCW(icoords)) { icoords.reverse } else { icoords }
    }.toList

    outerRing +: innerRings
  }

  def getLineStrings(polygon: Polygon): List[LineString] = {
    val polygon_id = polygon.getUserData.toString().split("\t")(0)
    val rings = getRings(polygon).zipWithIndex
    val hasHoles = rings.size > 1 match {
      case true => 1
      case _    => 0
    }
    rings.flatMap{ ring =>
      val ring_id = ring._2
      ring._1.zip(ring._1.tail).zipWithIndex.map{ pair =>
        val order = pair._2
        val coord1 = pair._1._1
        val coord2 = pair._1._2
        val coords = new CoordinateArraySequence(Array(coord1, coord2))

        val line = geofactory.createLineString(coords)
        line.setUserData(s"${polygon_id}\t${ring_id}\t${order}\t${hasHoles}")

        line
      }
    }
  }

  def getPolygons(polygon: Polygon): List[Polygon] = {
    getRings(polygon).map{ coords =>
      val poly = geofactory.createPolygon(coords)
      poly.setUserData(polygon.getUserData.toString())
      poly
    }
  }

  def getExteriorPolygon(polygon: Polygon): Polygon = {
    val poly = geofactory.createPolygon(polygon.getExteriorRing.getCoordinateSequence)
    poly.setUserData(polygon.getUserData.toString())
    poly
  }

  def edge2graphedge(edge: LineString ): GraphEdge = {
    val pts = edge.getCoordinates
    val hedge = Half_edge(Vertex(pts(0).x, pts(0).y), Vertex(pts(1).x, pts(1).y))
    val arr = edge.getUserData.toString().split("\t")
    hedge.id = arr(0)
    hedge.ring = arr(1).toInt
    hedge.order = arr(2).toInt

    new GraphEdge(pts, hedge)
  }

  def linestring2paircoord(line: LineString): List[(Coordinate, Coordinate)] = {
    val coords = line.getCoordinates
    coords.zip(coords.tail).toList
  }

  def linestring2graphedge(line: LineString, id: String = "*"): List[GraphEdge] = {
    linestring2paircoord(line).map{ pair =>
      val pts = Array(pair._1, pair._2)
      val hedge = Half_edge(Vertex(pts(0).x, pts(0).y), Vertex(pts(1).x, pts(1).y))
      hedge.id = id

      new GraphEdge(pts, hedge)
    }
  }

  def getCellsAtCorner(quadtree: StandardQuadTree[LineString], c: QuadRectangle): (List[QuadRectangle], Point) = {
    val region = c.lineage.takeRight(1).toInt
    val corner = region match {
      case 0 => geofactory.createPoint(new Coordinate(c.getEnvelope.getMaxX, c.getEnvelope.getMinY))
      case 1 => geofactory.createPoint(new Coordinate(c.getEnvelope.getMinX, c.getEnvelope.getMinY))
      case 2 => geofactory.createPoint(new Coordinate(c.getEnvelope.getMaxX, c.getEnvelope.getMaxY))
      case 3 => geofactory.createPoint(new Coordinate(c.getEnvelope.getMinX, c.getEnvelope.getMaxY))
    }
    val envelope = corner.getEnvelopeInternal
    val precision = 1 / precisionModel.getScale
    envelope.expandBy(precision)
    val cells = quadtree.findZones(new QuadRectangle(envelope)).asScala
      .filterNot(_.partitionId == c.partitionId).toList
      .sortBy(_.lineage.size)
    (cells, corner)
  }

  def getEdges(polygons: RDD[Polygon]): (RDD[LineString], Long) = {
    val edges = polygons.flatMap(getLineStrings).cache
    val nEdges = edges.count()
    (edges, nEdges)

  }

  def partitionEdges(edges: RDD[LineString])(implicit params: EdgePartitionerConf):
      (SpatialRDD[LineString], StandardQuadTree[LineString], Map[Int, QuadRectangle]) = {
    val edgesRDD = new SpatialRDD[LineString]()
    edgesRDD.setRawSpatialRDD(edges)
    edgesRDD.analyze()
    val boundary = new QuadRectangle(edgesRDD.boundaryEnvelope)
    val quadtree = new StandardQuadTree[LineString](boundary, 0, params.maxentries(), params.nlevels())
    for(edge <- edges.sample(false, params.sample(), 42).collect()) {
      quadtree.insert(new QuadRectangle(edge.getEnvelopeInternal), edge)
    }
    quadtree.assignPartitionIds()
    quadtree.assignPartitionLineage()
    val EdgePartitioner = new QuadTreePartitioner(quadtree)
    edgesRDD.spatialPartitioning(EdgePartitioner)
    edgesRDD.spatialPartitionedRDD.rdd.cache
    val nEdgesRDD = edgesRDD.spatialPartitionedRDD.rdd.count()
    val cells = quadtree.getLeafZones.asScala.map(cell => (cell.partitionId.toInt -> cell)).toMap

    (edgesRDD, quadtree, cells)
  }
  
  def getSegments(edgesRDD: SpatialRDD[LineString], cells: Map[Int, QuadRectangle]): RDD[LineString] = {
    val segments = edgesRDD.spatialPartitionedRDD.rdd.mapPartitionsWithIndex{ case (index, edges) =>
      val cell = envelope2Polygon(cells.get(index).get.getEnvelope)
      val g1 = edges.map(edge2graphedge).toList
      val g2 = linestring2graphedge(cell.getExteriorRing, "*")

      SweepLine.getGraphEdgeIntersections(g1, g2).flatMap{ gedge =>
        gedge.getLineStrings
      }.filter(line => line.coveredBy(cell)).toIterator
    }.cache
    val nSegments = segments.count()
    segments
  }

  def getHalf_edges(segments: RDD[LineString]): (RDD[Half_edge], Long) = {
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

  def getVertices(half_edges: Iterator[Half_edge]): Vector[Vertex] = {
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

  import scala.annotation.tailrec
  @tailrec
  def getNodes(start: Half_edge, end: Half_edge, v: Vector[Half_edge]): Vector[Half_edge] = {
    if(start == end){
      v :+ end
    } else {
      getNodes(start.next, end, v :+ start)
    }
  }

  @tailrec
  def connectHedges(v: Vector[Half_edge], r: Vector[Vector[Half_edge]]): Vector[Vector[Half_edge]] = {
    if(v.isEmpty){
      r
    } else {
      val node = v.head
      val nodes = getNodes(node, node.prev, Vector.empty[Half_edge])
      val newV = v.filterNot(nodes.contains)
      val newR = nodes +: r
      connectHedges(newV, newR)
    }
  }

  def getHedgesList(vertices: Vector[Vertex]): Vector[Vector[Half_edge]] = {
    val hedges = vertices.flatMap(_.getHalf_edges).toVector
    connectHedges(hedges, Vector.empty[Vector[Half_edge]])
  }

  def preprocessFacesAndHedges(vertices: Vector[Vertex], index: Int): Vector[(Face, Vector[Half_edge])] = {
    getHedgesList(vertices).map{ hedges =>
      val ids = hedges.map(_.id).distinct
      val id = ids.size match {
        case 2 => ids.filter(_ != "*").head
        case 1 => ids.head
        case _ => ids.mkString("|")
      }
      (id, hedges.map{ h => h.id = id; h })
    }
      .filter(_._1 != "*")
      .map{ case (id, hedges) =>
        val hedge = hedges.head
        val face = Face(id, index)
        face.id = id
        face.outerComponent = hedge
        val new_hedges = hedges.map{ h => h.face = face; h }

        (face, new_hedges)
      }
  }

  def getHedges(pre: Vector[(Face, Vector[Half_edge])]): Vector[Half_edge] = {
    pre.flatMap(_._2)
  }

  def getFaces(pre: Vector[(Face, Vector[Half_edge])]): Vector[Face] = {
    pre.map(_._1)
      .groupBy(_.id) // Grouping multi-parts
      .map{ case (id, faces) =>
        val polys = faces.sortBy(_.area).reverse
        val outer = polys.head
        outer.innerComponents = polys.tail.toVector

        outer
      }.toVector
  }

  def getLocalDCELs(half_edges: RDD[Half_edge]):
      (RDD[(Vector[Vertex], Vector[Half_edge], Vector[Face])], Long) = {
    val dcel = half_edges.mapPartitionsWithIndex{ case (index, half_edges) =>
      val vertices = getVertices(half_edges)

      val preFacesAndHedges = preprocessFacesAndHedges(vertices, index)

      val hedges = getHedges(preFacesAndHedges)
      val faces  = getFaces(preFacesAndHedges)

      List( (vertices, hedges, faces) ).toIterator
    }.cache
    val nDcel = dcel.count()
    (dcel, nDcel)
  }

  /************************************
   * The main function...
   ************************************/
  def main(args: Array[String]) = {
    implicit val params = new EdgePartitionerConf(args)
    implicit val debugState = params.debug()
    val input = params.input()
    val offset = params.offset()
    val quote = params.quote()

    debug{
      val precision = 1 / precisionModel.getScale
      logger.info(s"Using Scale=${precisionModel.getScale} and Precision=${precision}")
    }

    // Starting session...
    val spark = timer{"Starting session"}{
      SparkSession.builder()
        .config("spark.default.parallelism", 3 * 120)
        .config("spark.serializer",classOf[KryoSerializer].getName)
        .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
        .config("spark.scheduler.mode", "FAIR")
        .appName("EdgePartitioner")
        .getOrCreate()
    }
    import spark.implicits._
    val appID = spark.sparkContext.applicationId
    val startTime = spark.sparkContext.startTime

    // Reading data...
    val (polygons, nPolygons) = timer{"Reading data"}{
      val polygons = spark.read.textFile(input).rdd.zipWithUniqueId().map{ case (line, i) =>
        val arr = line.split("\t")
        val userData = List(s"$i") ++ (0 until arr.size).filter(_ != offset).map(i => arr(i))
        val wkt = if(quote){
          arr(offset).replaceAll("\"", "")
        } else {
          arr(offset)
        }
        val polygon = new WKTReader(geofactory).read(wkt)
        polygon.setUserData(userData.mkString("\t"))
        polygon.asInstanceOf[Polygon]
      }.cache
      val nPolygons = polygons.count()
      (polygons, nPolygons)
    }

    // Extracting edges...
    val (edges, nEdges) = timer{"Extracting edges"}{
      getEdges(polygons)
    }

    // Partitioning edges...
    val (edgesRDD, quadtree, cells) = timer{"Partitioning edges"}{
      partitionEdges(edges)
    }

    // Getting segments...
    val segments = timer{"Getting segments"}{
      getSegments(edgesRDD, cells)
    }

    // Getting half edges...
    val (half_edges, nHalf_edges) = timer{"Getting half edges"}{
      getHalf_edges(segments)
    }

    // Getting local DCEL's...
    val (dcel, nDcel) = timer{"Getting local DCEL's"}{
      getLocalDCELs(half_edges)
    }

    debug{
      logger.info(s"Partitions on DCEL: ${dcel.getNumPartitions}")

      val vertices = dcel.flatMap{_._1}
      logger.info(s"Number of vertices: ${vertices.count()}")
      val hedges = dcel.flatMap{_._2}
      logger.info(s"Number of half edges: ${hedges.count()}")
      val faces = dcel.flatMap{_._3}
      logger.info(s"Number of faces: ${faces.count()}")
    }

    if(params.save()){
      save{"/tmp/edgesCells.wkt"}{
        edgesRDD.spatialPartitionedRDD.rdd.mapPartitionsWithIndex{ case (index, partition) =>
          val cell = cells.get(index).get
          List(s"${index}\t${envelope2Polygon(cell.getEnvelope)}\t${partition.size}\n").toIterator
        }.collect()
      }

      save{"/tmp/edgesVertices.wkt"}{
        dcel.flatMap{ dcel =>
          dcel._1.map{ vertex =>
            s"${vertex.toWKT}\n"
          }
        }.collect()
      }

      save{"/tmp/edgesHedges.wkt"}{
        dcel.flatMap{ dcel =>
          dcel._2.map{ hedge =>
            s"${hedge.toWKT2}\n"
          }
        }.collect()
      }

      save{"/tmp/edgesFaces.wkt"}{
        dcel.flatMap{ dcel =>
          dcel._3.map{ face =>
            s"${face.id}\t${face.cell}\t${face.getGeometry._1.toText()}\t${face.getGeometry._2}\n"
          }
        }.collect()
      }
    }

    timer{"Clossing session"}{
      spark.close()
    }
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
  val partitions:  ScallopOption[Int]     = opt[Int]     (default = Some(512))
  val sample:      ScallopOption[Double]  = opt[Double]  (default = Some(0.25))
  val maxentries:  ScallopOption[Int]     = opt[Int]     (default = Some(500))
  val nlevels:     ScallopOption[Int]     = opt[Int]     (default = Some(6))
  val quote:       ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val local:       ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val debug:       ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val save:        ScallopOption[Boolean] = opt[Boolean] (default = Some(false))

  verify()
}

