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

object DCELTester2 {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def round(number: Double, scale: Double): Double =
    Math.round(number * scale) / scale;
  def roundEnvelope(envelope: Envelope, scale: Double = 100.0): Envelope = {
    val e = round(envelope.getMinX, scale)
    val w = round(envelope.getMaxX, scale)
    val s = round(envelope.getMinY, scale)
    val n = round(envelope.getMaxY, scale)
    new Envelope(e, w, s, n)
  }

  def main(args: Array[String]) = {
    // Starting session...
    logger.info("Starting session...")
    val appName = "DCELTester2"
    implicit val spark = SparkSession.builder()
        .config("spark.serializer",classOf[KryoSerializer].getName)
        .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
        .appName(appName)
        .getOrCreate()
    import spark.implicits._
    val startTime = spark.sparkContext.startTime
    val config = spark.sparkContext.getConf
    val appId: String = config.get("spark.app.id")
    logger.info(s"${appId}|${System.getProperty("sun.java.command")}")
    logger.info("Starting session... Done!")

    import scala.io.Source
    val model = new PrecisionModel(100)
    val geofactory = new GeometryFactory(model);
    val reader = new WKTReader(geofactory)
    val home = System.getProperty("user.home")
    val path = s"${home}/RIDIR/Code/Scripts/gadm"
    val boundaryBuff = Source.fromFile(s"${path}/boundary.wkt")
    val envelopeWKT = boundaryBuff.getLines.next
    val boundary = reader.read(envelopeWKT).getEnvelopeInternal
    boundaryBuff.close

    val quadtreeBuff = Source.fromFile(s"${path}/quadtree.wkt")
    val quads = quadtreeBuff.getLines.map{ line =>
      val geofactory = new GeometryFactory(model);
      val arr = line.split("\t")
      val polygon = reader.read(arr(0))
      val lineage = arr(1)
      val id = arr(2).toInt
      
      (id -> (lineage, polygon))
    }.toMap
    val quadtree = Quadtree.create(boundary, quads.values.map(_._1).toList)

    val cells = quadtree.getLeafZones.asScala.map{ cell =>
      val id = cell.partitionId.toInt
      val lineage = cell.lineage
      val envelope = roundEnvelope(cell.getEnvelope)
      val r = new org.datasyslab.geospark.spatialPartitioning.quadtree.QuadRectangle(envelope)
      r.lineage = lineage
      r.partitionId = id
      (id -> r)
    }.toMap
    val grids = spark.sparkContext
      .broadcast{
        cells.toSeq.sortBy(_._1).map{ case (i,q) =>
          envelope2polygon(q.getEnvelope)
        }
      }
    
    val edgesRDD_prime = spark.read.textFile(s"file://${path}/demo.tsv").rdd
      .mapPartitionsWithIndex{ case(index, lines) =>
        val reader = new WKTReader(geofactory)
        lines.map{ line =>
          val arr = line.split("\t")
          val pid = arr(0).toInt
          val data = s"${arr(1)}\t${arr(2)}\t${arr(3)}\t${arr(4)}"
          val edge = reader.read(arr(5)).asInstanceOf[LineString]
          edge.setUserData(data)

          (pid, edge)
        }
    }.cache()

    val pid2index = edgesRDD_prime.map(_._1).collect
      .distinct.zipWithIndex.map{ case(pid, index) => pid -> index}.toMap
    val index2pid = edgesRDD_prime.map(_._1).collect
      .distinct.zipWithIndex.map{ case(pid, index) => index -> pid}.toMap
    val npartitions = pid2index.keys.toList.length
    val edgesRDD = edgesRDD_prime.map{ case(pid, edge) =>
      (pid2index(pid), edge)
    }.partitionBy(new SimplePartitioner(npartitions))
      .map(_._2).cache()
    val nEdgesRDD = edgesRDD.count()

    logger.info(s"Total edges: $nEdgesRDD")
    logger.info(s"Total partitions: ${edgesRDD.getNumPartitions}")

    val dcels = edgesRDD.mapPartitionsWithIndex{ case (index, edgesIt) =>
      val edges = edgesIt.toVector
      val gA = edges.map(edge2graphedge).toList
      val pid = index2pid(index)
      val cell = envelope2polygon(cells(pid).getEnvelope)
      val gCellA = cell2gedges(cell)
      val cellEnvelope = cell.getEnvelopeInternal

      val edgesA = SweepLine.getEdgesOnCell(edges, cellEnvelope)
      edgesA.foreach{println}

      val Ah = SweepLine.getGraphEdgeIntersections(gA, gCellA).flatMap{_.getLineStrings}
      val At = transform(Ah, cell)
      val Am = merge(At)
      val Ap = pair(Am)
      val Af = filter(Ap)
      val dcelA = getLDCEL(Af, index, cells, grids, true)

      val r = (index, dcelA, edgesA)
      Iterator(r)
    }.cache
    val n = dcels.count()

    save{"/tmp/edgesEdges.wkt"}{
      edgesRDD.map{ edge =>
        val wkt = edge.toText
        val data = edge.getUserData

        s"$wkt\t$data\n"
      }.collect
    }
    save{"/tmp/edgesFaces.wkt"}{
      dcels.mapPartitionsWithIndex{ (index, dcels) =>
        val dcel = dcels.next

        dcel._2.faces.filter(_.id.substring(0, 1) != "F").map{ face =>
          val id = face.id
          val wkt = face.toPolygon.toText

          s"$wkt\t$id\t$index\n"
        }.toIterator
      }.collect
    }
    save{"/tmp/edgesHedges.wkt"}{
      dcels.mapPartitionsWithIndex{ (index, dcels) =>
        val dcel = dcels.next

        dcel._2.half_edges.map{ hedge =>
          val id = hedge.id
          val wkt = hedge.toLineString.toText

          s"$wkt\t$id\t$index\n"
        }.toIterator
      }.collect
    }
    save{"/tmp/edgesVertices.wkt"}{
      dcels.mapPartitionsWithIndex{ (index, dcels) =>
        val dcel = dcels.next

        dcel._2.vertices.map{ vertex =>
          val wkt = vertex.toWKT

          s"$wkt\t$index\n"
        }.toIterator
      }.collect
    }
    save{"/tmp/edgesAh.wkt"}{
      dcels.mapPartitionsWithIndex{ (index, dcels) =>
        val dcel = dcels.next

        dcel._3.map{ coord =>
          val wkt = geofactory.createPoint(coord)

          s"$wkt\n"          
        }.toIterator
      }.collect
    }
    
    spark.close
  }

  
}
