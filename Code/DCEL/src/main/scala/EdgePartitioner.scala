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
import scala.collection.mutable.{ListBuffer, TreeSet, ArrayBuffer, HashSet}
import scala.util.control.Breaks._

object EdgePartitioner{
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val startTime: Long = 0L
  private var precisionModel: PrecisionModel = new PrecisionModel(1000)
  private var geofactory: GeometryFactory = new GeometryFactory(precisionModel)
  private var precision: Double = 0.0001

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
    val ring = geofactory.createLinearRing(Array(p1,p2,p3,p4,p1))
    geofactory.createPolygon(ring)
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

  def roundAt(p: Int)(n : Double): Double = { val s = math pow (10, p); (math round n * s) / s }

  def getHalf_edges(polygon: Polygon): List[LineString] = {
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
    envelope.expandBy(precision)
    val cells = quadtree.findZones(new QuadRectangle(envelope)).asScala
      .filterNot(_.partitionId == c.partitionId).toList
      .sortBy(_.lineage.size)
    (cells, corner)
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
    val decimals = params.decimals()
    val ppartitions = params.ppartitions()
    val epartitions = params.epartitions()
    val quote = params.quote()
    val debug = params.debug()
    precisionModel = new PrecisionModel(math.pow(10, params.decimals()))
    geofactory = new GeometryFactory(precisionModel)
    //precision = 1 / precisionModel.getScale
    val master = params.local() match {
      case true  => s"local[${cores}]"
      case false => s"spark://${params.host()}:${params.port()}"
    }
    val gridType = params.grid() match {
      case "EQUALGRID" => GridType.EQUALGRID
      case "QUADTREE"  => GridType.QUADTREE
      case "KDBTREE"   => GridType.KDBTREE
    }

    if(debug){
      logger.info(s"Using Scale=${precisionModel.getScale} and Precision=${precision}")
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
    val polygonRDD = new SpatialRDD[Polygon]()
    val polygons = spark.read.textFile(input).repartition(200).rdd.zipWithUniqueId().map{ case (line, i) =>
      val arr = line.split("\t")
      val userData = List(s"$i") ++ (0 until arr.size).filter(_ != offset).map(i => arr(i))
      var wkt = arr(offset)
      if(quote){
        wkt = wkt.replaceAll("\"", "")
      }
      val polygon = new WKTReader(geofactory).read(wkt)
      polygon.setUserData(userData.mkString("\t"))
      polygon.asInstanceOf[Polygon]
    }.cache
    polygonRDD.setRawSpatialRDD(polygons)
    polygonRDD.analyze()
    val nPolygons = polygons.count()
    log(stage, timer, nPolygons, "END")

    timer = clocktime
    stage = "Extracting edges"
    log(stage, timer, 0, "START")
    val edges = polygons.map(_.asInstanceOf[Polygon]).flatMap(getHalf_edges).cache
    val nEdges = edges.count()
    log(stage, timer, nEdges, "END")

    timer = clocktime
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
    quadtree.assignPartitionLineage()
    val EdgePartitioner = new QuadTreePartitioner(quadtree)
    edgesRDD.spatialPartitioning(EdgePartitioner)
    edgesRDD.spatialPartitionedRDD.rdd.cache
    val quads = quadtree.getLeafZones.asScala.map(c => c.partitionId -> c.getEnvelope).toMap
    polygonRDD.spatialPartitioning(EdgePartitioner)
    polygonRDD.spatialPartitionedRDD.cache
    val cells = quadtree.getLeafZones.asScala.map(cell => (cell.partitionId -> cell)).toMap
    val nEdgesPartitions = edgesRDD.spatialPartitionedRDD.rdd.getNumPartitions
    log(stage, timer, nEdgesPartitions, "END")

    if(debug){
      var WKT = edgesRDD.spatialPartitionedRDD.rdd.mapPartitionsWithIndex{ case (index, partition) =>
        val cell = cells.get(index).get
        List(s"${index}\t${envelope2Polygon(cell.getEnvelope)}\t${partition.size}\n").toIterator
      }.collect()
      var filename = "/tmp/edgesCells.wkt"
      var f = new java.io.PrintWriter(filename)
      f.write(WKT.mkString(""))
      f.close
      logger.info(s"${filename} saved [${WKT.size}] records")

      /*
      WKT = polygons.map{ p =>
        val userData = p.getUserData.toString()
        s"${p.toText()}\t${userData}\n"
      }.collect()
      filename = "/tmp/edgesPolygons.wkt"
      f = new java.io.PrintWriter(filename)
      f.write(WKT.mkString(""))
      f.close
      logger.info(s"${filename} saved [${WKT.size}] records")
       */
    }

    //////////////////////////////////////////////////////////////////////////////
    timer = clocktime
    stage = "Solving empty cells"
    log(stage, timer, 0, "START")
    val edgeCount = edgesRDD.spatialPartitionedRDD.rdd.mapPartitionsWithIndex{ case (id, partition) =>
      List( (id, partition.size) ).toIterator
    }.collect().toMap
    var emptyCells = edgeCount.filter(_._2 == 0).map(cell => cells.get(cell._1).get)
      .map(c => c.partitionId -> EmptyCell(c, solve = false)).toMap
    if(debug){
      logger.info(s"Number of empty cell: ${emptyCells.size}")
    }
    for(emptyCellId <- emptyCells.keys){
      val emptyCell = emptyCells.get(emptyCellId).get
      if(!emptyCell.solve){
        var done = false
        var counter = 0
        var nextCellWithEdgesId: Int = -1
        var referenceCorner: Point = null
        var cellList = new ArrayBuffer[QuadRectangle]()
        cellList += emptyCell.cell
        while( !done ){
          val c = cellList.takeRight(1).head
          val (cellsReturned, corner) = getCellsAtCorner(quadtree, c)
          for(cell <- cellsReturned){
            if(edgeCount.get(cell.partitionId).get > 0){
              done = true
              counter = 200
              nextCellWithEdgesId = cell.partitionId
              referenceCorner = corner
            } else {
              cellList += cell
            }
          }
        }
        cellList.foreach{ cell =>
          val empty = emptyCells.get(cell.partitionId).get
          empty.solve = true
          empty.nextCellWithEdgesId = nextCellWithEdgesId
          empty.referenceCorner = referenceCorner
          emptyCells += cell.partitionId -> empty
        }        
      }
    }
    log(stage, timer, emptyCells.size, "END")
    //////////////////////////////////////////////////////////////////////////////

    if(debug){
      var WKT = emptyCells.toArray.map{ e =>
        val emptyCell = e._2
        s"${e._1}\t${emptyCell.nextCellWithEdgesId}\t${emptyCell.referenceCorner.toText()}\t${envelope2Polygon(emptyCell.cell.getEnvelope).toText()}\n"
      }
      var filename = "/tmp/edgesEmptyCells.wkt"
      var f = new java.io.PrintWriter(filename)
      f.write(WKT.mkString(""))
      f.close
      logger.info(s"${filename} saved [${WKT.size}] records")
    }

    timer = clocktime
    stage = "Getting segments"
    log(stage, timer, 0, "START")
    val segments = edgesRDD.spatialPartitionedRDD.rdd.mapPartitionsWithIndex{ case (index, edges) =>
      val cell = envelope2Polygon(cells.get(index).get.getEnvelope)
      val g1 = edges.map(edge2graphedge).toList
      val g2 = linestring2graphedge(cell.getExteriorRing, "*")

      SweepLine.getGraphEdgeIntersections(g1, g2).flatMap{ gedge =>
        gedge.getLineStrings
      }.filter(line => line.coveredBy(cell)).toIterator
    }.cache
    val nSegments = segments.count()
    log(stage, timer, nSegments, "END")

    timer = clocktime
    stage = "Getting half edges"
    log(stage, timer, 0, "START")
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
    log(stage, timer, nHalf_edges, "END")

    timer = clocktime
    stage = "Getting local DCEL's"
    log(stage, timer, 0, "START")
    val dcel = half_edges.mapPartitionsWithIndex{ case (index, half_edges) =>
      val vertices = half_edges.map(hedge => (hedge.v2, hedge)).toList
        .groupBy(_._1).toList.map{ v =>
          val vertex = v._1
          vertex.setHalf_edges(v._2.map(_._2))
          vertex
        }

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

      val hedges = vertices.flatMap(v => v.getHalf_edges)
      var faces = new ArrayBuffer[Face]()
      val half_edgeList = hedges.flatMap{ hedge =>
        var hedges = new ArrayBuffer[Half_edge]()
        var h = hedge
        do{
          hedges += h
          h = h.next
        }while(h != hedge)
        val ids = hedges.map(_.id).distinct.filter(_ != "*")
        val id = ids.size match {
          case 1 => ids.head
          case 0 => "*"
          case _ => ids.mkString("|")
        }
        hedge.id = id
        List( hedge ).toIterator
      }
        .filter(_.id != "*")
      
      var temp_half_edgeList = new ArrayBuffer[Half_edge]()
      temp_half_edgeList ++= half_edgeList
      for(temp_hedge <- temp_half_edgeList){
        val hedge = half_edgeList.find(_.equals(temp_hedge)).get
        if(hedge.face == null){
          val f = Face(hedge.id)
          f.outerComponent = hedge
          f.outerComponent.face = f
          f.id = hedge.id
          f.ring = hedge.ring
          var h = hedge
          do{
            half_edgeList.find(_.equals(h)).get.face = f
            h = h.next
          }while(h != f.outerComponent)
          faces += f
        }
      }
      val facesList = faces.groupBy(_.id).map{ case (id, faces) =>
        val f = faces.toList.sortBy(_.ring)
        val head = f.head
        val tail = f.tail
        head.innerComponent = tail
        head.outerComponents = f.filter(_.ring == 0)
        head.innerComponents = f.filter(_.ring != 0)

        head
      }.toList

      List( (vertices, hedges, facesList) ).toIterator
    }.cache
    val nDcel = dcel.count()
    log(stage, timer, nDcel, "END")


    ////////////////////////////////////////////////
    val nullCells = dcel.mapPartitionsWithIndex { case (id, partition) =>
      val faces = partition.next._3
      val cell = cells.get(id).get
      val cellPoly = envelope2Polygon(cell.getEnvelope)
      val facesEnvelopes = faces.map(_.getEnvelope).toArray
      val facesEnvelope = geofactory.createGeometryCollection(facesEnvelopes).getEnvelopeInternal
      facesEnvelope.expandBy(precision)
      val facesPoly = envelope2Polygon(facesEnvelope)
      val flag = cellPoly.getExteriorRing.intersects(facesPoly)
      List( (id, cellPoly.toText(), facesPoly.toText(), flag, faces.size) ).toIterator
    }.sortBy(_._1).collect()
    ////////////////////////////////////////////////


    if(debug){
      var WKT = dcel.flatMap{ dcel =>
        val faces = dcel._3
        faces.sortBy(_.id).map{ face =>
            s"${face.toWKT2}\t${face.innerComponent.size}\n"
        }
      }.collect()
      var filename = "/tmp/edgesFaces.wkt"
      var f = new java.io.PrintWriter(filename)
      f.write(WKT.mkString(""))
      f.close
      logger.info(s"${filename} saved [${WKT.size}] records")

      WKT = nullCells.map(n => s"${n._1}\t${n._2}\t${n._3}\t${n._4}\t${n._5}\n")
      filename = "/tmp/edgesNullCells.wkt"
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
  val decimals:    ScallopOption[Int]     = opt[Int]     (default = Some(6))
  val ppartitions: ScallopOption[Int]     = opt[Int]     (default = Some(512))
  val epartitions: ScallopOption[Int]     = opt[Int]     (default = Some(512))
  val esample:     ScallopOption[Double]  = opt[Double]  (default = Some(0.25))
  val eentries:    ScallopOption[Int]     = opt[Int]     (default = Some(500))
  val elevels:     ScallopOption[Int]     = opt[Int]     (default = Some(6))
  val quote:       ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val local:       ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val debug:       ScallopOption[Boolean] = opt[Boolean] (default = Some(false))

  verify()
}

