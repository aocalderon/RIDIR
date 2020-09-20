import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import com.vividsolutions.jts.algorithm.CGAlgorithms
import com.vividsolutions.jts.geom.{Coordinate, Envelope, Geometry}
import com.vividsolutions.jts.geom.{LineString, LinearRing, Point, Polygon}
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{Row, Dataset, SparkSession}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.spatialPartitioning.quadtree._
import org.datasyslab.geospark.spatialPartitioning.{KDBTree, KDBTreePartitioner}
import org.geotools.geometry.jts.GeometryClipper
import ch.cern.sparkmeasure.TaskMetrics
import org.rogach.scallop._
import org.slf4j.{Logger, LoggerFactory}
import DCELBuilder._
import CellManager2._
import SingleLabelChecker._

object DCELMerger{
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")
  implicit val model: PrecisionModel = new PrecisionModel(1000)
  implicit val geofactory: GeometryFactory = new GeometryFactory(model);
  val precision: Double = 1 / model.getScale

  case class Settings(spark: SparkSession, params: DCELMergerConf, conf: SparkConf,
    startTime: Long, appId: String, cores: Int, executors: Int)

  def clocktime = System.currentTimeMillis()

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

  def save(filename: String)(content: Seq[String]): Unit = {
    val start = clocktime
    val f = new java.io.PrintWriter(filename)
    f.write(content.mkString(""))
    f.close
    val end = clocktime
    val time = "%.2f".format((end - start) / 1000.0)
    logger.info(s"Saved ${filename} in ${time}s [${content.size} records].")
  }

  def envelope2polygon(e: Envelope): Polygon = {
    val minX = e.getMinX()
    val minY = e.getMinY()
    val maxX = e.getMaxX()
    val maxY = e.getMaxY()
    val p1 = new Coordinate(minX, minY)
    val p2 = new Coordinate(maxX, minY)
    val p3 = new Coordinate(maxX, maxY)
    val p4 = new Coordinate(minX, maxY)
    geofactory.createPolygon(Array(p1,p2,p3,p4,p1))
  }

  def roundEnvelope(envelope: Envelope, scale: Double = 100.0): Envelope = {
    val e = round(envelope.getMinX, scale)
    val w = round(envelope.getMaxX, scale)
    val s = round(envelope.getMinY, scale)
    val n = round(envelope.getMaxY, scale)
    new Envelope(e, w, s, n)
  }
  private def round(number: Double, scale: Double): Double = Math.round(number * scale) / scale;
  
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

  ////////////////////////////////////////////////////////////////////////////////////////
  // Functions
  ////////////////////////////////////////////////////////////////////////////////////////
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

  def removeTag(id: String, tag: String): String = {
    id.split("\\|").filterNot(_ == tag).mkString("|")
  }

  def preprocess(vertices: Vector[Vertex], index: Int,
    cells: Map[Int, QuadRectangle], grids: Broadcast[Seq[Polygon]]):
      Vector[(Face, Vector[Half_edge])] = {

    val cell = envelope2polygon(cells(index).getEnvelope)
    getHedgesList(vertices).zipWithIndex.map{ case(hedges, i) =>
      val id = parseIds(hedges.map(_.id), i)
      
      (id, hedges.map{ h => h.id = id; h })
    }
      .map{ case (id, hedges) =>
        val hedge = hedges.head
        val face = Face(id, index)
        face.id = id
        face.outerComponent = hedge
        val poly = face.toPolygon()
        face.exterior = face.isCellBorder(grids)
        val new_hedges = hedges.map{ h =>
          h.face = face
          if(face.exterior){
            h.id = "F"
          }
          h
        }

        (face, new_hedges)
      }
  }

  def getHedges2(pre: Vector[(Face, Vector[Half_edge])]): Vector[Half_edge] = {
    pre.flatMap(_._2).map{ h =>
      h.id = removeTag(h.id, "C")
      h
    }
  }

  def getFaces2(pre: Vector[(Face, Vector[Half_edge])]): Vector[Face] = {
    pre.map(_._1).map{ f =>
      f.id = removeTag(f.id, "C")
      f
    }
      .filterNot{ f => f.exterior }
      .toVector
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
        .filterNot(_ == "F")
        .mkString("|")
      f
    }
  }

  ////////////////////////////////////////////////////////////////////////////////////
  // main functions
  ////////////////////////////////////////////////////////////////////////////////////
  def getLDCEL(h: Vector[Half_edge], i: Int,
    cells: Map[Int, QuadRectangle], grids: Broadcast[Seq[Polygon]],
    keepEmptyFaces: Boolean = false): LDCEL = {

    val vertices = getVerticesByV2(h.toIterator)
    val pre = preprocess(vertices, i, cells, grids)
    val hedges = getHedges2(pre)
    val faces  = if(keepEmptyFaces){
      getFaces2(pre)
    } else {
      getFaces2(pre).filter(_.id.substring(0,1) != "F")
    }

    LDCEL(0, vertices, hedges, faces, i)
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
    val appName = s"DCEL_P${partitions}"
    implicit val spark = SparkSession.builder()
        .config("spark.serializer",classOf[KryoSerializer].getName)
        .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
        .appName(appName)
        .getOrCreate()
    import spark.implicits._
    val startTime = spark.sparkContext.startTime
    val config = spark.sparkContext.getConf
    val local = config.get("spark.master").contains("local")
    val (appId, cores, executors) = if(local){
      val appId = config.get("spark.app.id")
      val cores = config.get("spark.master").split("\\[")(1).replace("]","").toInt
      val command = System.getProperty("sun.java.command")
      logger.info(s"${appId}|${command}")

      (appId, cores, 1)
    } else {
      val appId = config.get("spark.app.id").takeRight(4)
      val cores = config.get("spark.executor.cores").toInt
      val executors = config.get("spark.executor.instances").toInt
      val command = System.getProperty("sun.java.command")
      logger.info(s"${appId}|${command}")

      (appId, cores, executors)
    }
    implicit val settings = Settings(spark,
      params, config, startTime, appId, cores, executors)
    val metrics = TaskMetrics(spark)
    def getPhaseMetrics(metrics: TaskMetrics, phaseName: String): Dataset[Row] = {
      metrics.createTaskMetricsDF()
        .withColumn("appId", functions.lit(appId))
        .withColumn("phaseName", functions.lit(phaseName))
        .orderBy("launchTime")
    }    
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
      logger.info(s"Edges in A: ${edgesA.count()}")
      val edgesB = getEdges(polygonsB)
      logger.info(s"Edges in B: ${edgesB.count()}")
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
        qtree.assignPartitionLineage()
        (edgesRDD, qtree)
      }
      val cells = quadtree.getLeafZones.asScala.map(cell => (cell.partitionId.toInt -> cell)).toMap
      edgesRDD.spatialPartitionedRDD.rdd.cache
      val nEdgesRDD = edgesRDD.spatialPartitionedRDD.rdd.count()
      (edgesRDD, nEdgesRDD, cells, quadtree)
    }
    val grids = spark.sparkContext
      .broadcast{
        cells.toSeq.sortBy(_._1).map{ case (i,q) => envelope2polygon(q.getEnvelope)}
      }
  
    log(f"Total number of partitions|${cells.size}")
    log(f"Total number of edges in raw|${edgesRDD.rawSpatialRDD.count()}")
    log(f"Total number of edges in spatial|${edgesRDD.spatialPartitionedRDD.count()}")

    
    save{"/tmp/envelope.wkt"}{
      val boundary = edgesRDD.boundary()
      val wkt = envelope2polygon(boundary).toText()
      Array(wkt)
    }
    save{"/tmp/quadtree.wkt"}{
      val cells = quadtree.getLeafZones.asScala
        .map(leaf => leaf.partitionId -> leaf)
        .toMap
      edgesRDD.spatialPartitionedRDD.rdd.mapPartitionsWithIndex{ (index, it) =>
        val lineage = cells(index).lineage
        val wkt = envelope2polygon(cells(index).getEnvelope).toText
        Iterator(s"$wkt\t$lineage\t$index\n")
      }.collect()
    }

    /*****************************************************************************/
    edgesRDD.spatialPartitionedRDD.rdd.mapPartitionsWithIndex{ case(index, edges) =>
      edges.map{ edge =>
        val data = edge.getUserData         // polygon_id, ring_id, order, hasHoles
        val wkt = edge.toText

        s"$index\t$data\t$wkt"
      }
    }.toDF.write
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .text("tmp/edges")

    //**
    metrics.begin()
    val dcelsRDD = timer{"Extracting A and B DCELs"}{
      val dcels = edgesRDD.spatialPartitionedRDD.rdd
        .mapPartitionsWithIndex{ case (index, edgesIt) =>
          val edges = edgesIt.toVector
          val gA = edges.filter(isA).map(edge2graphedge).toList
          val gB = edges.filter(isB).map(edge2graphedge).toList
          val cell = envelope2polygon(cells(index).getEnvelope)
          val gCellA = cell2gedges(cell)
          val gCellB = cell2gedges(cell)

          val Ah = SweepLine.getGraphEdgeIntersections(gA, gCellA).flatMap{_.getLineStrings}
          val At = transform(Ah, cell)
          val Am = merge(At)
          val Ap = pair(Am)
          val Af = filter(Ap)
          val dcelA = getLDCEL(Af, index, cells, grids, true)

          val Bh = SweepLine.getGraphEdgeIntersections(gB, gCellB).flatMap{_.getLineStrings}
          val Bt = transform(Bh, cell)
          val Bm = merge(Bt)
          val Bp = pair(Bm)
          val Bf = filter(Bp)
          val dcelB = getLDCEL(Bf, index, cells, grids, true)

          val r = (dcelA, dcelB)
          Iterator(r)
        }.cache
      val n = dcels.count()
      dcels
    }
    metrics.end()
    val phaseExtract = getPhaseMetrics(metrics, "Extract DCELs")
    phaseExtract.repartition(1).write
      .mode("overwrite")
      .format("csv")
      .option("delimiter", "\t")
      .option("header", true)
      .save("hdfs:///user/acald013/logs/sdcel")

    /******************************************************/
    dcelsRDD.mapPartitionsWithIndex{ case(index, iter) =>
      val dcel = iter.next()

      dcel._1.faces.map{ face =>
        val id = face.id
        val arr = id.split("\\|").filter(_.substring(0,1) != "F")
        val n = arr.length
        val polys = face.getPolygons
        val wkts = polys.map{_.toText}.mkString("|")

        (id, n, wkts)
      }.toIterator
    }.filter(_._2 > 1)
      .map{ case(id, n, wkts) => s"$id\t$n\t$wkts"}
      .toDF().write
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .text("tmp/A_prime")
    /******************************************************/
    
/*
    debug{
      save{"/tmp/edgesHA.wkt"}{
        dcelsRDD.map(_._1).flatMap{ dcel =>
          dcel.half_edges.map(h => s"${h.toLineString.toText()}\t${h.id}\t${dcel.index}\n")
        }.collect()
      }
      save{"/tmp/edgesFA.wkt"}{
        dcelsRDD.map(_._1).flatMap{ dcel =>
          dcel.faces.map(f => s"${f.getGeometry._1.toText()}\t${f.id}\t${dcel.index}\t${f.isCellBorder(grids)}\n")
        }.collect()
      }
      save{"/tmp/edgesHB.wkt"}{
        dcelsRDD.map(_._2).flatMap{ dcel =>
          dcel.half_edges.map(h => s"${h.toLineString.toText()}\t${h.id}\t${dcel.index}\n")
        }.collect()
      }
      save{"/tmp/edgesFB.wkt"}{
        dcelsRDD.map(_._2).flatMap{ dcel =>
          dcel.faces.map(f => s"${f.getGeometry._1.toText()}\t${f.id}\t${dcel.index}\t${f.isCellBorder(grids)}\n")
        }.collect()
      }
    }
 */

    // Calling methods in CellManager.scala
    val (dcelARDD, dcelBRDD) = timer{"Updating empty cells"}{

      ////
      logger.info("Starting CellManager...")
      val dcelARDD0 = updateCellsWithoutId(dcelsRDD.map{_._1}, quadtree,
        label = "A", debug = false)
      logger.info("Starting CellManager... Done!")
      ////

      val dcelARDD = dcelARDD0
        .mapPartitionsWithIndex{ case(index, iter) =>
          val dcel = iter.next()
          val faces = dcel.faces.groupBy(_.id) // Grouping multi-parts
            .map{ case (id, f) =>
              val polys = f.sortBy(_.area).reverse
              val outer = polys.head
              outer.innerComponents = polys.tail.toVector

              outer
            }.toVector
          faces.foreach{ f =>
            f.exterior = f.isCellBorder(grids)
          }
          Iterator(dcel.copy(faces = faces))
        }
        .cache()
      dcelARDD.count()

      ////
      logger.info("Starting CellManager...")
      val dcelBRDD0 = updateCellsWithoutId(dcelsRDD.map{_._2}, quadtree,
        label = "B", debug = false)
      logger.info("Starting CellManager... Done!")
      ////

      val dcelBRDD = dcelBRDD0.mapPartitionsWithIndex{ case(index, iter) =>
          val dcel = iter.next()
          val faces = dcel.faces.groupBy(_.id) // Grouping multi-parts
            .map{ case (id, f) =>
              val polys = f.sortBy(_.area).reverse
              val outer = polys.head
              outer.innerComponents = polys.tail.toVector

              outer
            }.toVector
          faces.foreach{ f =>
            f.exterior = f.isCellBorder(grids)
          }
          Iterator(dcel.copy(faces = faces))
        }
        .cache()
      dcelBRDD.count()
      (dcelARDD, dcelBRDD)
    }

    /****************************************************************************/
    dcelARDD.mapPartitionsWithIndex{ case(index, iter) =>
      val dcel = iter.next()

      dcel.faces.map{ face =>
        val id = face.id
        val isF = id.substring(0,1) == "F"
        val polys = face.getPolygons
        val n = polys.length

        if(n > 1 & !isF){
          val wkts = polys.map{_.toText}.mkString("|")
          s"$index\t$id\t$n\t$wkts"
        } else {
          ""
        }
      }.toIterator
    }.toDF().write
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .text("tmp/facesA")

    dcelBRDD.mapPartitionsWithIndex{ case(index, iter) =>
      val dcel = iter.next()

      dcel.faces.map{ face =>
        val id = face.id
        val isF = id.substring(0,1) == "F"
        val polys = face.getPolygons
        val n = polys.length

        if(n > 1 & !isF){
          val wkts = polys.map{_.toText}.mkString("|")
          s"$index\t$id\t$n\t$wkts"
        } else {
          ""
        }
      }.toIterator
    }.toDF().write
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .text("tmp/facesB")
    /****************************************************************************/    

    debug{
      save{"/tmp/edgesHAprime.wkt"}{
        dcelARDD.flatMap{ dcel =>
          dcel.half_edges.map(h => s"${h.toLineString.toText()}\t${h.id}\t${dcel.index}\n")
        }.collect()
      }
      save{"/tmp/edgesFAprime.wkt"}{
        dcelARDD.flatMap{ dcel =>
          dcel.faces.map(f => s"${f.getGeometry._1.toText()}\t${f.id}\t${dcel.index}\n")
        }.collect()
      }
      save{"/tmp/edgesHBprime.wkt"}{
        dcelBRDD.flatMap{ dcel =>
          dcel.half_edges.map(h => s"${h.toLineString.toText()}\t${h.id}\t${dcel.index}\n")
        }.collect()
      }
      save{"/tmp/edgesFBprime.wkt"}{
        dcelBRDD.flatMap{ dcel =>
          dcel.faces.map(f => s"${f.getGeometry._1.toText()}\t${f.id}\t${dcel.index}\n")
        }.collect()
      }
    }
 
    metrics.begin()
    val dcels_prime = timer{"Merging DCELs"}{
      val dcels = dcelARDD.zipPartitions(dcelBRDD, preservesPartitioning=true){ (iterA, iterB) =>
        val dcelA = iterA.next() 
        val gA = dcelA.half_edges.map(hedge2gedge).toList
        val dcelB = iterB.next()
        val gB = dcelB.half_edges.map(hedge2gedge).toList

        val Ch = SweepLine.getGraphEdgeIntersections(gA, gB).flatMap{_.getLineStrings}
        val Ct = transform2(Ch)
        val Cm = merge(Ct)
        val Cp = pair(Cm)
        val Cf = filter(Cp)

        val mergedDCEL = getLDCEL(Cf, dcelA.index, cells, grids)

        val dcels = (mergedDCEL, dcelA, dcelB)
        Iterator(dcels)
      }.cache
      dcels.count()
      dcels
    }
    metrics.end()
    val phaseMerge = getPhaseMetrics(metrics, "Merge DCELs")

    debug{
      save{s"/tmp/edgesFC.wkt"}{
        dcels_prime.map(_._1).flatMap{ dcel =>
          dcel.faces.map{ face =>
            s"${face.getGeometry._1.toText()}\t${face.id}\t${face.cell}\t${face.getGeometry._2}\n"
          }
        }.collect()
      }
      save{s"/tmp/edgesHC.wkt"}{
        dcels_prime.map(_._1).flatMap{ dcel =>
          dcel.half_edges.map{ h =>
            s"${h.toLineString.toText()}\t${h.id}\n"
          }
        }.collect()
      }
    }

    /**********************************************************************/
    logger.info("Saving merge dcel and quadtree...")
    val faceRDD = dcels_prime.mapPartitionsWithIndex{ case(index, iter) =>
      val dcels = iter.next()
      val dcel = dcels._1

      dcel.faces.map{ face =>
        val id = face.id
        val wkt = face.getGeometry._1.toText

        s"$wkt\t$id\t$index"
      }.toIterator
    }
    faceRDD.toDF().write
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .text("tmp/faces")

    save{"/tmp/quadtree.lin"}{
      quadtree.getLeafZones.asScala.map(_.lineage + "\n")
    }
    logger.info("Saving merge dcel and quadtree... Done!")
    /**********************************************************************/

    
    // Calling methods in SingleLabelChecker...
    val dcels = timer{"Checking single-label faces"}{
      val dcels0 = dcels_prime
        .mapPartitionsWithIndex{ case(index, iter) =>
          val dcels = iter.next()
          val dcel = dcels._1

          val hasHoles = dcel.faces.exists(!_.isCCW)
          if(hasHoles){
            val outers = dcel.faces.filter(_.isCCW)
            val inners = dcel.faces.filter(!_.isCCW)

            for{
              outer <- outers
              inner <- inners if {
                try{
                  val a = outer.toPolygon()
                  val b = inner.toPolygon()
                  a.covers(b) && !a.equals(b)
                } catch{
                  case e: com.vividsolutions.jts.geom.TopologyException => {
                    val wktA = outer.toPolygon.toText()
                    val wktB = inner.toPolygon.toText()
                    logger.info(s"Outers:\n $wktA \n")
                    logger.info(s"Inners:\n $wktB \n")
                    false
                  }
                }
              }
            } yield {
              inner.id = outer.id
            }
            val faces = dcel.faces.groupBy(_.id) // Grouping multi-parts
              .map{ case (id, f) =>
                val polys = f.sortBy(_.area).reverse
                val outer = polys.head
                outer.innerComponents = polys.tail.toVector

                outer
              }.toVector

            Iterator( (dcel.copy(faces = faces), dcels._2, dcels._3) )
          } else {
            val faces = dcel.faces.groupBy(_.id) // Grouping multi-parts
              .map{ case (id, f) =>
                if(id == "A5|B14"){
                  f.map(_.getGeometry._1.toText).foreach(println)
                }
                // Removing very small faces...
                val polys = f//.filter(_.getGeometry._1.getArea >= 0.001)
                  .sortBy(_.area).reverse
                val outer = polys.head
                outer.innerComponents = polys.tail.toVector

                outer
              }.toVector
            Iterator( (dcel.copy(faces = faces), dcels._2, dcels._3) )
          }
        }
        .cache

      val dcels = checkSingleLabels(dcels0)
      //val dcels = dcels0
      dcels.count()
      dcels
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
            s"${h.toLineString.toText()}\t${h.id}\n"
          }
        }.collect()
      }
    }

    ///////////////////////////////////////////////////////////////////////////////////////
    // overlay functions
    ///////////////////////////////////////////////////////////////////////////////////////

    def mergePolygons(a: String, b:String): String = {
      val reader = new WKTReader(geofactory)
      val pa = reader.read(a)
      val pb = reader.read(b)
      if(pa.getArea < 0.001 || pa.getArea.isNaN()) b
      else if(pb.getArea < 0.001 || pb.getArea.isNaN()) a
      else{
        geofactory.createGeometryCollection(Array(pa, pb)).buffer(0.0).toText()
       // pa.union(pb).toText()
      }
    }

    def intersection(dcel: LDCEL): Vector[(String, Geometry)] = {
      dcel.faces.filter(_.id.split("\\|").size == 2).map(f => (f.id, f.getGeometry._1))
    }
    def union(dcel: LDCEL): Vector[(String, Geometry)] = {
      dcel.faces.map(f => (f.id, f.getGeometry._1))
    }
    def symmetricDifference(dcel: LDCEL): Vector[(String, Geometry)] = {
      dcel.faces.filter(_.id.split("\\|").size == 1).map(f => (f.id, f.getGeometry._1))
    }
    
    def differenceA(dcel: LDCEL): Vector[(String, Geometry)] = {
      symmetricDifference(dcel).filter(_._1.substring(0, 1) == "A")
    }
    def differenceB(dcel: LDCEL): Vector[(String, Geometry)] = {
      symmetricDifference(dcel).filter(_._1.substring(0, 1) == "B")
    }
    
    def overlapOp(dcel: RDD[LDCEL], op: (LDCEL) => Vector[(String, Geometry)],
        filename: String = "/tmp/overlay.wkt"){

      val results = dcel.flatMap{op}//.filter{_._2.getArea > 0.0001}
        .map{ case (id, geom) =>
          (id, geom.toText())
        }
        .reduceByKey{ case(a, b) =>
          mergePolygons(a, b)
        }
      results.cache()
      logger.info(s"Overlay operation done! [${results.count()} results].")

      debug{
        save{filename}{
          results.map{ case(id, wkt) => s"$wkt\t$id\n" }.collect()
        }
      }
    }

    val output_path = "/tmp/edges"
    val dcel = dcels.map(_._1).cache()
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
    

    // Closing session...
    logger.info("Closing session...")
    debug{
      /*
      val phases = phaseExtract.union(phaseMerge)
      phases.repartition(1).write
        .mode("overwrite")
        .format("csv")
        .option("delimiter", "\t")
        .option("header", true)
        .save("hdfs:///user/acald013/logs/sdcel")
       */
    }
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

  verify()
}

