package edu.ucr.dblab.sdcel

import scala.annotation.tailrec
import com.vividsolutions.jts.geom.LineString
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.slf4j.{Logger, LoggerFactory}
import edu.ucr.dblab.sdcel.quadtree._
import edu.ucr.dblab.sdcel.geometries._
import PartitionReader._

import Utils._

object DCELBuilder2 {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def getLDCELs(edgesRDD: RDD[LineString], cells: Map[Int, Cell])
    (implicit geofactory: GeometryFactory, logger: Logger, spark: SparkSession,
    settings: Settings)
      : RDD[Iterable[Half_edge]] = {

    edgesRDD.mapPartitionsWithIndex{ case (pid, edgesIt) =>
      val cell = cells(pid).mbr
      val edges = edgesIt.toVector
      val (outerEdges, innerEdges) = edges.partition{ edge =>
        cell.intersects(edge)
      }
      val outer  = SweepLine2.getHedgesTouchingCell(outerEdges.toVector, cell, pid)
      val inner  = SweepLine2.getHedgesInsideCell(innerEdges.toVector)

      val hedges = SweepLine2.merge(outer, inner, pid)

      Iterator(hedges)
    }
  }

  /*
  def main(args: Array[String]) = {
    // Starting session...
    logger.info("Starting session...")
    implicit val params = new Params(args)
    implicit val spark = SparkSession.builder()
        .config("spark.serializer",classOf[KryoSerializer].getName)
        .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
        .getOrCreate()
    import spark.implicits._
    val conf = spark.sparkContext.getConf
    val appId = conf.get("spark.app.id")
    implicit val settings = Settings(
      tolerance = params.tolerance(),
      debug = params.debug(),
      appId = appId
    )
    val command = System.getProperty("sun.java.command")
    log(command)

    val model = new PrecisionModel(settings.scale)
    implicit val geofactory = new GeometryFactory(model)
    logger.info("Starting session... Done!")

    // Reading data...
    val (quadtree, cells) = readQuadtree[LineString](params.quadtree(), params.boundary())
    val edgesRDD = readEdges(params.input1(), quadtree, "A")
    logger.info("Reading data... Done!")

    if(params.debug()){
      save{"/tmp/edgesCells.wkt"}{
        cells.values.map{ cell =>
          val wkt = envelope2polygon(cell.mbr.getEnvelopeInternal).toText
          val id = cell.id
          val lineage = cell.lineage
          s"$wkt\t$id\t$lineage\n"
        }.toList
      }
    }

    // Getting LDCELs...
    val dcels = getLDCELs(edgesRDD, cells).persist()
    val n = dcels.count()
    logger.info("Getting LDCELs done!")

    if(params.debug()){
      save{"/tmp/edgesH.wkt"}{
        dcels.mapPartitionsWithIndex{ (index, dcelsIt) =>
          val dcel = dcelsIt.next
          dcel.map{ h =>
            val wkt = h.getPolygon.toText
            val pid = h.data.polygonId
            val rid = h.data.ringId
            val eid = h.data.edgeId
            
            s"$wkt\t$pid:$rid:$eid\t$index\n"
          }.toIterator
        }.collect
      }

      val facesRDD = getFaces(dcels)
      save("/tmp/edgesF.wkt"){
        facesRDD.map{ face =>
          s"${face.getGeometry.toText}\t${face.polygonId}\t${face.inners.size}\n"
        }.collect
      }
    }
     
    spark.close
  }

  def getFaces(dcels: RDD[Iterable[Half_edge]])
      (implicit geofactory: GeometryFactory): RDD[Face] = {
    @tailrec
    def matchHoles(holes: List[Face], exteriors: Vector[Face]): Vector[Face] = {
      holes match {
        case Nil => exteriors
        case head +: tail => {
          val hole = head.toPolygon
          val exterior = exteriors
            .find{ exterior =>
              val polygon = exterior.toPolygon
              hole.coveredBy(polygon) }.get
          exterior.inners = exterior.inners :+ head
          matchHoles(tail, exteriors)
        }
      }
    }

    dcels.mapPartitionsWithIndex{ (index, dcelsIt) =>
      val dcel = dcelsIt.next
      dcel.map{Face}
        .groupBy(_.polygonId).values
        .map{ faces =>
          val (holes, exteriors) = faces.toVector.partition(_.isHole)
          (holes.toList, exteriors)
        }.toIterator
    }.flatMap{ case(holes, exteriors) => matchHoles(holes, exteriors)}
  }
   */

  def save(filename: String)(content: Seq[String]): Unit = {
    val start = clocktime
    val f = new java.io.PrintWriter(filename)
    f.write(content.mkString(""))
    f.close
    val end = clocktime
    val time = "%.2f".format((end - start) / 1000.0)
    logger.info(s"Saved ${filename} in ${time}s [${content.size} records].")
  }

  private def clocktime = System.currentTimeMillis()
}
