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

  case class Settings(spark: SparkSession, params: DCELMergerConf, conf: SparkConf,
    startTime: Long, appId: String, cores: Int, executors: Int)

  def timer[A](msg: String)(code: => A)(implicit settings: Settings): A = {
    val start = clocktime
    val result = code
    val end = clocktime

    val tick = (end - settings.startTime) / 1e3
    val time = (end - start) / 1e3
    val partitions = settings.params.partitions()

    val appId = settings.appId
    val cores = settings.cores
    val executors = settings.executors
    val log = f"DCELMerger|$appId|$executors%2d|$cores%2d|$partitions%5d|$msg%-30s|$time%6.2f"
    logger.info(log)

    result
  }

  def debug[A](code: => A)(implicit settings: Settings): Unit = {
    if(settings.params.debug()){
      code
    }
  }

  def log(msg: String)(implicit settings: Settings): Unit = {
    logger.info(f"DEBUG|${settings.appId}|$msg")
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

  def readPolygons(spark: SparkSession, input: String, offset: Int, tag: String): (RDD[Polygon], Long) = {
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

  def getQuadTree(edges: RDD[LineString], boundary: QuadRectangle)(implicit settings: Settings): StandardQuadTree[LineString] = {
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

  def getCellsAtCorner(quadtree: StandardQuadTree[LineString], c: QuadRectangle, precision: Double): (List[QuadRectangle], Point) = {
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
        .appName("DCELMerger")
        .getOrCreate()
    import spark.implicits._
    val startTime = spark.sparkContext.startTime
    val config = spark.sparkContext.getConf
    val local = config.get("spark.master").contains("local")
    val (appId, cores, executors) = if(local){
      val appId = config.get("spark.app.id")
      val cores = config.get("spark.master").split("\\[")(1).replace("]","").toInt
      (appId, cores, 1)
    } else {
      val command = System.getProperty("sun.java.command")
      logger.info(command)
      val appId = config.get("spark.app.id").takeRight(4)
      val cores = config.get("spark.executor.cores").toInt
      val executors = config.get("spark.executor.instances").toInt
      (appId, cores, executors)
    }
    implicit val settings = Settings(spark, params, config, startTime, appId, cores, executors)
    
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
      save{"/tmp/edgesA.wkt"}{
        polygonsA.map{ a =>
          s"${a.toText}\t${a.getUserData}\n"
        }.collect()
      }
      save{"/tmp/edgesB.wkt"}{
        polygonsB.map{ b =>
          s"${b.toText}\t${b.getUserData}\n"
        }.collect()
      }
    }

    // Partitioning edges...
    val (edgesRDD, nEdgesRDD, cells, quadtree) = timer{"Partitioning edges"}{
      val edgesA = getEdges(polygonsA)
      val edgesB = getEdges(polygonsB)
      val edges = edgesA.union(edgesB)
      val (edgesRDD, quadtree: StandardQuadTree[LineString]) = if(params.custom()){
        val (edgesRDD, boundary) = setSpatialRDD(edges)
        val qtree = getQuadTree(edges, boundary)
        val partitioner = new QuadTreePartitioner(qtree)
        edgesRDD.spatialPartitioning(partitioner)
        (edgesRDD, qtree)
      } else {
        val (edgesRDD, boundary) = setSpatialRDD(edges)
        edgesRDD.spatialPartitioning(GridType.QUADTREE, params.partitions())
        val qtree = edgesRDD.partitionTree.asInstanceOf[StandardQuadTree[LineString]]
        (edgesRDD, qtree)
      }
      val cells = quadtree.getLeafZones.asScala.map(cell => (cell.partitionId.toInt -> cell)).toMap
      edgesRDD.spatialPartitionedRDD.rdd.cache
      val nEdgesRDD = edgesRDD.spatialPartitionedRDD.rdd.count()
      (edgesRDD, nEdgesRDD, cells, quadtree)
    }

    

    debug{
      log(f"Total number of partitions|${cells.size}")
      log(f"Total number of edges in raw|${edgesRDD.rawSpatialRDD.count()}")
      log(f"Total number of edges in spatial|${edgesRDD.spatialPartitionedRDD.count()}")
      save{"/tmp/edgesCells.wkt"}{
        edgesRDD.spatialPartitionedRDD.rdd.mapPartitionsWithIndex{ case (index, partition) =>
          val cell = cells.get(index).get
          List(s"${envelope2Polygon(cell.getEnvelope).toText()}\t${index}\t${partition.size}\n").toIterator
        }.collect()
      }      
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // Functions
    //////////////////////////////////////////////////////////////////////////////////////////////////
    def transform(hedges: List[LineString], cell: Polygon, index: Int = -1): Vector[Half_edge] = {
        hedges.filter(line => line.coveredBy(cell)).flatMap{ segment =>
          val arr = segment.getUserData.toString().split("\t")
          val coords = segment.getCoordinates
          val v1 = Vertex(coords(0).x, coords(0).y)
          val v2 = Vertex(coords(1).x, coords(1).y)
          val h1 = Half_edge(v1, v2)
          h1.id = arr(0)
          h1.label = index.toString()
          val t1 = Half_edge(v2, v1)
          t1.id = arr(0).substring(0, 1)
          t1.isTwin = true
          t1.label = index.toString()
          Vector(h1, t1)
        }.toVector
    }

    def transform2(hedges: List[LineString]): Vector[Half_edge] = {
        hedges.flatMap{ segment =>
          val arr = segment.getUserData.toString().split("\t")
          val coords = segment.getCoordinates
          val v1 = Vertex(coords(0).x, coords(0).y)
          val v2 = Vertex(coords(1).x, coords(1).y)
          val h1 = Half_edge(v1, v2)
          h1.id = arr(0)
          val t1 = Half_edge(v2, v1)
          t1.id = arr(0).substring(0, 1)
          t1.isTwin = true
          Vector(h1, t1)
        }.toVector
    }
 
    def merge(hedges: Vector[Half_edge]): Vector[Half_edge] = {
      hedges.map(h => ((h.v1, h.v2), h)).groupBy(_._1).values.map(_.map(_._2))
        .flatMap{ hedges =>
          if(hedges.size > 1){
            val id = hedges.map(_.id).mkString("|")
            val h = hedges.head
            h.id = id
            Vector(h)
          } else {
            hedges
          }
        }.toVector
    }

    def pair(hedges: Vector[Half_edge]): Vector[Half_edge] = {
      hedges.map{ h =>
        if (h.isTwin) ((h.v2, h.v1), h) else ((h.v1, h.v2), h)
      }.groupBy(_._1).values.map(_.map(_._2))
        .flatMap{ h =>
          h(0).twin = h(1)
          h(1).twin = h(0)
          h
        }.toVector
    }

    def filter(hedges: Vector[Half_edge]): Vector[Half_edge] = {
      hedges.map{ h =>
        if(h.id.contains("|")){
          val id = h.id.split("\\|")
            .filter(_ != "A")
            .filter(_ != "B")
            .filter(_ != "*")
            .filter(_.substring(0,1) != "F")
            .mkString("|")
          h.id = if(id == "") "C" else id
          h
        } else {
          h
        }
      }
    }

    def hedge2gedge(hedge: Half_edge): GraphEdge = {
      val pts = Array(hedge.v1.toCoordinate, hedge.v2.toCoordinate)
      new GraphEdge(pts, hedge)
    }

    def cell2gedges(cell: Polygon): List[GraphEdge] = {
      val segments = cell.getExteriorRing.getCoordinates
      segments.zip(segments.tail).map{ case(c1, c2) =>
        val v1 = Vertex(c1.x, c1.y)
        val v2 = Vertex(c2.x, c2.y)
        val h = Half_edge(v1, v2)
        h.id = "C"
        hedge2gedge(h)
      }.toList      
    }

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
    
    def parseIds(ids: Vector[String], i: Int): String = {
      val id = ids.distinct
        .filter(_ != "A")
        .filter(_ != "B")
        .filter(_ != "C")
        .filter(_ != "D" )
        .filter(_ != "E" )
        .sorted.mkString("|")
      if(id == "") "F" + i else id.split("\\|").distinct.mkString("|")
    }

    def preprocess(vertices: Vector[Vertex], index: Int = -1, tag: String = ""):
        Vector[(Face, Vector[Half_edge])] = {
      getHedgesList(vertices).zipWithIndex.map{ case(hedges, i) =>
        val id = parseIds(hedges.map(_.id), i)
        
        (id, hedges.map{ h => h.id = id; h })
      }
        .map{ case (id, hedges) =>
          val hedge = hedges.head
          val face = Face(id, index)
          face.id = id
          face.tag = tag
          face.outerComponent = hedge
          face.exterior = !CGAlgorithms.isCCW(face.toPolygon().getCoordinates)
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

    def getFaces2(pre: Vector[(Face, Vector[Half_edge])]): Vector[Face] = {
      pre.map(_._1)
        .filterNot{ _.exterior }
        .groupBy(_.id) // Grouping multi-parts
        .map{ case (id, faces) =>
          val polys = faces.sortBy(_.area).reverse
          val outer = polys.head
          outer.innerComponents = polys.tail.toVector

          outer
        }.toVector
    }

    def doublecheckFaces(faces: Vector[Face]): Vector[Face] = {
      val f_prime = faces.filter(_.id.split("\\|").size == 1)
        .map{ face =>
          val hedges = face.getHedges
          face.id = face.isSurroundedBy match{
            case Some(id) => List(face.id, id).sorted.mkString("|")
            case None => face.id
          }
          face
        }

      faces.map{f =>
        f.id = f.id.split("\\|")
          .filterNot(_ == "C")
          .filterNot(_ == "F")
          .mkString("|")
        f
      }
    }

    ////////////////////////////////////////////////////////////////////////////////////
    // main functions
    ////////////////////////////////////////////////////////////////////////////////////
    def getLDCEL(h: Vector[Half_edge]): LDCEL = {
      val vertices = getVerticesByV2(h.toIterator)
      val pre = preprocess(vertices)
      val hedges = getHedges2(pre)
      val faces  = doublecheckFaces(getFaces2(pre))
        .filter(_.id.substring(0,1) != "F")

      LDCEL(0, vertices, hedges, faces)
    }

    val dcelsRDD = timer{"Extracting A and B DCELs"}{
      val dcels = edgesRDD.spatialPartitionedRDD.rdd.mapPartitionsWithIndex{ case (index, edgesIt) =>
        val edges = edgesIt.toVector
        val gA = edges.filter(isA).map(edge2graphedge).toList
        val gB = edges.filter(isB).map(edge2graphedge).toList
        val cell = envelope2Polygon(cells.get(index).get.getEnvelope)
        val gCellA = cell2gedges(cell)
        val gCellB = cell2gedges(cell)

        val Ah = SweepLine.getGraphEdgeIntersections(gA, gCellA).flatMap{_.getLineStrings}
        val At = transform(Ah, cell)
        val Am = merge(At)
        val Ap = pair(Am)
        val Af = filter(Ap)
        val dcelA = getLDCEL(Af)

        val Bh = SweepLine.getGraphEdgeIntersections(gB, gCellB).flatMap{_.getLineStrings}
        val Bt = transform(Bh, cell)
        val Bm = merge(Bt)
        val Bp = pair(Bm)
        val Bf = filter(Bp)
        val dcelB = getLDCEL(Bf)

        val r = (dcelA, dcelB)
        Iterator(r)
      }.cache
      val n = dcels.count()
      dcels
    }
    val dcelARDD = dcelsRDD.map{_._1}
    val dcelBRDD = dcelsRDD.map{_._2}

    save{"/tmp/edgesHedgesByIndex.wkt"}{
      dcelARDD.mapPartitionsWithIndex{ case(index, iter) =>
        val dcel = iter.next()
        dcel.half_edges.map(h => s"${h.toWKT3}\t$index\n").toIterator
      }.collect()
    }

    def getNextCellWithEdges(MA: Map[Int, Int]): List[(Int, Int, Point)] = {
      var result = new ListBuffer[(Int, Int, Point)]()
      MA.filter{ case(index, size) => size == 0 }.map{ case(index, size) =>
        var cellList = new ListBuffer[QuadRectangle]()
        var nextCellWithEdges = -1
        var referenceCorner = geofactory.createPoint(new Coordinate(0,0))
        cellList += cells(index)
        var done = false
        while(!done){
          val c = cellList.last
          val (ncells, corner) = getCellsAtCorner(quadtree, c, precision)
          for(cell <- ncells){
            val nedges = MA(cell.partitionId)
            if(nedges > 0){
              nextCellWithEdges = cell.partitionId
              referenceCorner = corner
              done = true
            } else {
              cellList += cell
            }
          }
        }
        for(cell <- cellList){
          println(s"${cell}\t${nextCellWithEdges}\t${referenceCorner.toText()}")
          val r = (cell.partitionId.toInt, nextCellWithEdges, referenceCorner)
          result += r
        }
      }
      result.toList
    }

    val MA = dcelARDD.mapPartitionsWithIndex{ case(index, iter) =>
      val dcel = iter.next()
      val r = (index, dcel.half_edges.filter(_.id.substring(0,1) != "F").size)
      Iterator(r)
    }.collect().toMap

    val Aecells = getNextCellWithEdges(MA)
    val Afcells = dcelARDD.mapPartitionsWithIndex{ case(index, iter) =>
      val dcel = iter.next()
      val r = if(Aecells.map(_._2).contains(index)){
        val ecells = Aecells.filter(_._2 == index).map{ e =>
          val id = e._1
          val corner = e._3.buffer(0.001)
          (id, corner)
        }
        val fcells = dcel.faces.map{ f =>
          val id = f.id
          val face = f.getGeometry._1
          (id, face)
        }
        for{
          ecell <- ecells
          fcell <- fcells if fcell._2.intersects(ecell._2)
        } yield {
          (ecell._1, fcell._1)
        }
      } else {
        Vector.empty[(Int, String)]
      }
      r.toIterator
    }.collect()
    dcelARDD.mapPartitionsWithIndex{ case(index, iter) =>
      val dcel = iter.next()
      if(Afcells.map(_._1).contains(index)){
        val cell = Afcells.filter(_._1 == index)
        dcel.faces.map{ f =>
          f.id = cell.head._2
          f
        }
      }
      Iterator(dcel)
    }.flatMap(_.faces.map(f => s"${f.getGeometry._1.toText()}\t${f.id}")).foreach{println}

    val MB = dcelBRDD.mapPartitionsWithIndex{ case(index, iter) =>
      val dcel = iter.next()
      val r = (index, dcel.half_edges.filter(_.id.substring(0,1) != "F").size)
      Iterator(r)
    }.collect().toMap

    //getNextCellWithEdges(MB).foreach { println }

    debug{
      save{"/tmp/edgesHAprime.wkt"}{
        dcelARDD.flatMap{ dcel =>
          dcel.half_edges.map(h => s"${h.toWKT3}\t${h.twin.id}\n")
        }.collect()
      }
      save{"/tmp/edgesFAprime.wkt"}{
        dcelARDD.flatMap{ dcel =>
          dcel.faces.map(f => s"${f.getGeometry._1.toText()}\t${f.id}\n")
        }.collect()
      }
      save{"/tmp/edgesHBprime.wkt"}{
        dcelBRDD.flatMap{ dcel =>
          dcel.half_edges.map(h => s"${h.toWKT3}\t${h.twin.id}\n")
        }.collect()
      }
      save{"/tmp/edgesFBprime.wkt"}{
        dcelBRDD.flatMap{ dcel =>
          dcel.faces.map(f => s"${f.getGeometry._1.toText()}\t${f.id}\n")
        }.collect()
      }
    }

    val dcels_prime = timer{"Merging Halfedges..."}{
      dcelARDD.zipPartitions(dcelBRDD, preservesPartitioning=true){ (iterA, iterB) =>
        val dcelA = iterA.next() 
        val gA = dcelA.half_edges.map(hedge2gedge).toList
        val dcelB = iterB.next()
        val gB = dcelB.half_edges.map(hedge2gedge).toList

        val Ch = SweepLine.getGraphEdgeIntersections(gA, gB).flatMap{_.getLineStrings}
        val Ct = transform2(Ch)
        val Cm = merge(Ct)
        val Cp = pair(Cm)
        val Cf = filter(Cp)

        val mergedDCEL = getLDCEL(Cf)

        val dcels = (mergedDCEL, dcelA, dcelB)
        Iterator(dcels)
      }.cache
    }
    
    val dcels = timer{"Postprocessing..."}{
      dcels_prime.map{ dcels =>
        val facesM = dcels._1.faces
        val singleA = facesM.filter(_.id.substring(0,1) == "A")
        val facesB = if(singleA.size > 0){
          Some(dcels._3.faces)
        } else {
          None
        }

        facesB match {
          case Some(faces) => {
            for{
              B <- faces
              A <- singleA if B.toPolygon().getCentroid.coveredBy(A.toPolygon())
            } yield {
              A.id = A.id + "|" + B.id
            }
          }
          case None => //logger.warn("No single labels with A")
        }

        val singleB = facesM.filter(_.id.substring(0,1) == "B")
        val facesA = if(singleB.size > 0){
          Some(dcels._2.faces)
        } else {
          None
        }

        facesA match {
          case Some(faces) => {
            for{
              A <- faces
              B <- singleB if A.toPolygon().getCentroid.coveredBy(B.toPolygon())
            } yield {
              B.id = B.id + "|" + A.id
            }
          }
          case None => //logger.warn("No single labels with B")
        }
        dcels
      }.cache
        //.flatMap(_._1.faces.map(f => s"${f.getGeometry._1.toText()}\t${f.id}\n")).foreach(print)
    }

    debug{
      save{s"/tmp/edgesFaces.wkt"}{
        dcels.map(_._1).flatMap{ dcel =>
          dcel.faces.map{ face =>
            s"${face.getGeometry._1.toText()}\t${face.id}\t${face.cell}\t${face.getGeometry._2}\n"
          }
        }.collect()
      }
      save{s"/tmp/edgesHedges.wkt"}{
        dcels.map(_._1).flatMap{ dcel =>
          dcel.half_edges.map{ h =>
            s"${h.toWKT3}\t${h.isTwin}\n"
          }
        }.collect()
      }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // overlay functions
    //////////////////////////////////////////////////////////////////////////////////////////////////

    /*
    def mergePolygons(a: String, b:String): String = {
      val reader = new WKTReader(geofactory)
      val pa = reader.read(a)
      val pb = reader.read(b)
      pa.union(pb).toText()
    }

    def intersection(dcel: LDCEL): Vector[(String, Geometry)] = {
      dcel.faces.filter(_.id.split("\\|").size == 2).map(f => (f.id, f.getGeometry._1))
    }
    def union(dcel: LDCEL): Vector[Face] = {
      dcel.faces
    }
    def symmetricDifference(dcel: LDCEL): Vector[(String, Geometry)] = {
      dcel.faces.filter(_.id.split("\\|").size == 1).map(f => (f.id, f.getGeometry._1))
    }
    
    def differenceA(dcel: LDCEL): Vector[Face] = {
      symmetricDifference(dcel).filter(_.id.size > 1).filter(_.id.substring(0, 1) == "A")
    }
    def differenceB(dcel: LDCEL): Vector[Face] = {
      symmetricDifference(dcel).filter(_.id.size > 1).filter(_.id.substring(0, 1) == "B")
    }
    
    def overlapOp(dcel: RDD[LDCEL], op: (LDCEL) => Vector[(String, Geometry)],
      filename: String = "/tmp/overlay.wkt"){
      save{filename}{
        dcel.flatMap{op}.map{case (id, geom) => (id, geom.toText())}
        .reduceByKey(mergePolygons)
          .map{ case(id, wkt) => s"$wkt\t$id\n" }.collect()
      }
    }

    val output_path = "/tmp/edges"
    timer{"Intersection"}{
      overlapOp(dcel, intersection, s"${output_path}OpIntersection.wkt")
    }
    timer{"Symmetric"}{
      overlapOp(dcel, symmetricDifference, s"${output_path}OpSymmetric.wkt")
    }
    timer{"Union"}{
      overlapOp(dcel, union, s"${output_path}OpUnion.wkt")
    }
    timer{"Diff A"}{
      overlapOp(dcel, differenceA, s"${output_path}OpDifferenceA.wkt")
    }
    timer{"Diff B"}{
      overlapOp(dcel, differenceB, s"${output_path}OpDifferenceB.wkt")
    }
     */

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

