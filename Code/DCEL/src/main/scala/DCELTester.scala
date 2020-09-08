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
import org.apache.spark.sql.functions
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.spatialPartitioning.quadtree._
import org.geotools.geometry.jts.GeometryClipper
import ch.cern.sparkmeasure.TaskMetrics
import org.rogach.scallop._
import org.slf4j.{Logger, LoggerFactory}
import DCELBuilder._
import CellManager2._
import SingleLabelChecker._

import Quadtree.create
import DCELMerger._

object DCELTester {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val model: PrecisionModel = new PrecisionModel(1000)
  val geofactory: GeometryFactory = new GeometryFactory(model);
  val precision: Double = 1 / model.getScale

  case class Settings(spark: SparkSession, params: DCELMergerConf, conf: SparkConf,
    startTime: Long, appId: String, cores: Int, executors: Int)

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

    import scala.io.Source
    val reader = new WKTReader(geofactory)
    val path = "/home/acald013/RIDIR/Code/Scripts/gadm"
    val boundaryBuff = Source.fromFile(s"${path}/boundary.wkt")
    val envelopeWKT = boundaryBuff.getLines.next
    val boundary = reader.read(envelopeWKT).getEnvelopeInternal
    boundaryBuff.close
    val quadtreeBuff = Source.fromFile(s"${path}/quadtree.wkt")

    val quads = quadtreeBuff.getLines.map{ line =>
      val arr = line.split("\t")
      val polygon = reader.read(arr(0)).asInstanceOf[Polygon]
      val lineage = arr(1)
      val id = arr(2).toInt
      
      (id -> (lineage, polygon))
    }.toMap
    val npartitions = quads.keys.toList.length
    val quadtree = Quadtree.create(boundary, quads.values.map(_._1).toList)

    val cells = quadtree.getLeafZones.asScala.map{ cell =>
      val id = cell.partitionId.toInt
      val lineage = cell.lineage
      val envelope = cell.getEnvelope
      val r = new org.datasyslab.geospark.spatialPartitioning.quadtree.QuadRectangle(envelope)
      r.lineage = lineage
      r.partitionId = id
      (id -> r)
    }.toMap
    val grids = spark.sparkContext
      .broadcast{
        cells.toSeq.sortBy(_._1).map{ case (i,q) => envelope2polygon(q.getEnvelope)}
      }
    
    
    val edgesRDD = spark.read.textFile("file:///tmp/edgesSample.wkt").rdd
      .mapPartitionsWithIndex{ case(index, lines) =>
        val reader = new WKTReader(geofactory)
        lines.map{ line =>
          val arr = line.split("\t")
          val pid = arr(0).toInt
          val id = arr(1)
          val data = s"$id\t${arr(2)}\t${arr(3)}\t${arr(4)}"
          val edge = reader.read(arr(5)).asInstanceOf[LineString]
          edge.setUserData(data)

          (id, pid, edge)
        }
    }
      .filter(_._1.substring(0,1) == "A")
      .map{ case(id, pid, edge) => (pid, edge)}
      .filter{ case(pid, edge) => pid >= 480 && pid <=489 }
      .partitionBy(new SimplePartitioner(npartitions))
      .map(_._2).persist()
    val nEdgesRDD = edgesRDD.count()

    logger.info(s"Total edges: $nEdgesRDD")

    save{"/tmp/edgesSample.wkt"}{
      edgesRDD.mapPartitionsWithIndex{ (index, edges) =>
        edges.map{ edge =>
          val data = edge.getUserData
          val wkt = edge.toText
          s"$index\t$data\t$wkt\n"
        }
      }.collect
    }

    val dcels = edgesRDD.mapPartitionsWithIndex{ case (index, edgesIt) =>
      val edges = edgesIt.toVector
      val gA = edges.map(edge2graphedge).toList
      val cell = envelope2polygon(cells(index).getEnvelope)
      val gCellA = cell2gedges(cell)

      val Ah = SweepLine.getGraphEdgeIntersections(gA, gCellA).flatMap{_.getLineStrings}
      val At = transform(Ah, cell)
      val Am = merge(At)
      val Ap = pair(Am)
      val Af = filter(Ap)
      val dcelA = getLDCEL(Af, index, cells, grids, true)

      val r = (index, dcelA)
      Iterator(r)
    }.cache
    val n = dcels.count()

    println(n)
    save{"/tmp/edgesFaces.wkt"}{
      dcels.mapPartitionsWithIndex{ (index, dcels) =>
        val query = "0123"
        val sample = cells(index).lineage
        if(sample.length >= query.length && sample.substring(0,4) == query){
          val dcel = dcels.next

          dcel._2.faces.filter(_.id.substring(0, 1) != "F").map{ face =>
            val id = face.id
            val wkt = face.toPolygon.toText

            s"$wkt\t$id\t$index\n"
          }.toIterator
        } else {
          List.empty[String].toIterator
        }
      }.collect
    }
 
    spark.close
  }
}
