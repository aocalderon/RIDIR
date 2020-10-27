package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geom.LineString
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.TaskContext
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.slf4j.{Logger, LoggerFactory}
import edu.ucr.dblab.sdcel.quadtree._
import edu.ucr.dblab.sdcel.geometries._
import PartitionReader._
import DCELBuilder2.getLDCELs
import DCELMerger2.merge

object SDCEL {
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
    val (edgesRDDA, quadtree, cells) = readEdges(params.input1())
    val edgesRDDB = readEdges(params.input2(), quadtree)
    edgesRDDA.persist()
    val nEdgesRDDA = edgesRDDA.count()
    edgesRDDB.persist()
    val nEdgesRDDB = edgesRDDB.count()
    logger.info("Reading data... Done!")

    // Getting LDCELs...
    val dcelsA = getLDCELs(edgesRDDA, cells).persist()
    val nA = dcelsA.count()
    logger.info("Getting LDCELs for A done!")
    val dcelsB = getLDCELs(edgesRDDB, cells).persist()
    val nB = dcelsB.count()
    logger.info("Getting LDCELs for B done!")

    // Merging DCELs...
    val sdcel = dcelsA.zipPartitions(dcelsB, preservesPartitioning=true){ (iterA, iterB) =>
      val partitionId = TaskContext.getPartitionId
      val t0 = clocktime

      val A = iterA.next.toList
      val B = iterB.next.toList

      val hedges = merge(A, B)

      val t1 = clocktime
      val time = "%.2f".format((t1 - t0) / 1000.0)
      logger.info(s"Partition $partitionId in $time s")

      hedges.toIterator
    }.persist()
    val nSDcel = sdcel.count()
    logger.info("Merging DCELs... done!")

    if(params.debug()){
      save{"/tmp/edgesCells.wkt"}{
        cells.values.map{ cell =>
          val wkt = envelope2polygon(cell.mbr.getEnvelopeInternal).toText
          val id = cell.id
          val lineage = cell.lineage
          s"$wkt\t$id\t$lineage\n"
        }.toList
      }
      save{"/tmp/edgesHA.wkt"}{
        dcelsA.mapPartitionsWithIndex{ (index, dcelsIt) =>
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
      save{"/tmp/edgesHB.wkt"}{
        dcelsB.mapPartitionsWithIndex{ (index, dcelsIt) =>
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
      save{"/tmp/edgesH.wkt"}{
        sdcel.mapPartitionsWithIndex{ (index, dcelsIt) =>
          dcelsIt.map{ case(tag, h) =>
            val wkt = h.getPolygon.toText
            val pid = h.data.polygonId
            val rid = h.data.ringId
            val eid = h.data.edgeId
            
            s"$wkt\t$tag\t$index\n"
          }.toIterator
        }.collect
      }
    }
     
    spark.close
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
