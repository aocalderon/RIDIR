package edu.ucr.dblab.sdcel

import scala.collection.JavaConverters._
import com.vividsolutions.jts.geom.{Coordinate, Envelope}
import com.vividsolutions.jts.geom.{LineString, Polygon, LinearRing}
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.slf4j.{Logger, LoggerFactory}
import edu.ucr.dblab.sdcel.quadtree._
import edu.ucr.dblab.sdcel.geometries._
import PartitionReader._

object DCELMerger2 {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]) = {
    // Starting session...
    logger.info("Starting session...")
    implicit val spark = SparkSession.builder()
        .config("spark.serializer",classOf[KryoSerializer].getName)
        .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
        .getOrCreate()
    import spark.implicits._
    implicit val params = new Params(args)
    val model = new PrecisionModel(1000)
    implicit val geofactory = new GeometryFactory(model)
    logger.info("Starting session... Done!")

    // Reading data...
    val (edgesRDD, quadtree, cells) = readEdges(params.input2())
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
    val dcels = edgesRDD.mapPartitionsWithIndex{ case (index, edgesIt) =>
      val cell = cells(index).mbr
      val envelope = cell.getEnvelopeInternal

      val edges = edgesIt.toVector
      println(index)

      val (outerEdges, innerEdges) = edges.partition{ edge =>
        cell.intersects(edge)
      }

      val outer = SweepLine2.getHedgesTouchingCell(outerEdges.toVector, cell)
      val inner = SweepLine2.getHedgesInsideCell(innerEdges.toVector)
      val hedges = SweepLine2.merge(outer, inner)

      val r = (index, outer, inner, hedges)
      Iterator(r)
    }.cache
    val n = dcels.count()
    logger.info("Getting LDCELs done!")

    if(params.debug()){
      save{"/tmp/edgesHout.wkt"}{
        dcels.mapPartitionsWithIndex{ (index, dcelsIt) =>
          val dcel = dcelsIt.next
          dcel._2.map{ h =>
            val wkt = makeWKT(h)
            val pid = h.head.data.polygonId
            val start = h.head.data.edgeId
            val end = h.last.data.edgeId
            
            s"$wkt\t$pid:$start:$end\t$index\n"
          }.toIterator
        }.collect
      }
      save{"/tmp/edgesHin.wkt"}{
        dcels.mapPartitionsWithIndex{ (index, dcelsIt) =>
          val dcel = dcelsIt.next
          dcel._3.map{ h =>
            val wkt = makeWKT(h)
            val pid = h.head.data.polygonId
            val start = h.head.data.edgeId
            val end = h.last.data.edgeId
            
            s"$wkt\t$pid:$start:$end\t$index\n"
          }.toIterator
        }.collect
      }
      save{"/tmp/edgesH.wkt"}{
        dcels.mapPartitionsWithIndex{ (index, dcelsIt) =>
          val dcel = dcelsIt.next
          dcel._4.map{ h =>
            val wkt = h.getPolygon.toText
            val pid = h.data.polygonId
            val rid = h.data.ringId
            val eid = h.data.edgeId
            
            s"$wkt\t$pid:$rid:$eid\t$index\n"
          }.toIterator
        }.collect
      }
    }
     
    spark.close
  }

  def makeWKT(hedges: List[Half_edge])(implicit geofactory: GeometryFactory): String = {
    val coords = hedges.map{_.v1} :+ hedges.last.v2
    geofactory.createLineString(coords.toArray).toText
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
  private def clocktime = System.currentTimeMillis()
}
