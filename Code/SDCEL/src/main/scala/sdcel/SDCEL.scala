package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geom.LineString
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.TaskContext

import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.slf4j.{Logger, LoggerFactory}
import ch.cern.sparkmeasure.TaskMetrics

import edu.ucr.dblab.sdcel.quadtree._
import edu.ucr.dblab.sdcel.geometries._

import PartitionReader._
import DCELBuilder2.getLDCELs
import DCELMerger2.merge2
import DCELOverlay2._
import Utils._

object SDCEL {
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
      local = params.local(),
      persistance = params.persistance() match {
        case 0 => StorageLevel.NONE
        case 1 => StorageLevel.MEMORY_ONLY
        case 2 => StorageLevel.MEMORY_ONLY_SER
        case 3 => StorageLevel.MEMORY_ONLY_2
        case 4 => StorageLevel.MEMORY_ONLY_SER_2
      },
      appId = appId
    )
    val command = System.getProperty("sun.java.command")
    log(command)

    logger.info(s"Scale: ${settings.scale}")
    val model = new PrecisionModel(settings.scale)
    implicit val geofactory = new GeometryFactory(model)

    val (quadtree, cells) = readQuadtree[Int](params.quadtree(), params.boundary())

    val qtag = params.qtag()
    save{s"/tmp/edgesCells_${qtag}.tsv"}{
      cells.values.map{ cell =>
        val wkt = envelope2polygon(cell.mbr.getEnvelopeInternal).toText
        val id = cell.id
        val lineage = cell.lineage
        s"$wkt\t$id\t$lineage\n"
      }.toList
    }
    logger.info(s"${appId}|npartitions=${cells.size}")
    log("Starting session... Done!")

    // Reading data...
    val edgesRDDA = readEdges(params.input1(), quadtree, "A").cache
    val nEdgesRDDA = edgesRDDA.count()
    logger.info(s"${appId}|nEdgesA=${nEdgesRDDA}")

    save(s"/tmp/cellsA_${qtag}.tsv"){
      edgesRDDA.mapPartitionsWithIndex{ (pid, it) =>
        val n = it.size
        val wkt = cells(pid).wkt
        val r = s"$pid\t$n\t$wkt\n"
        Iterator(r)
      }.collect
    }

    val edgesRDDB = readEdges(params.input2(), quadtree, "B").cache
    val nEdgesRDDB = edgesRDDB.count()
    logger.info(s"${appId}|nEdgesB=${nEdgesRDDB}")
    log("Reading data... Done!")

    save(s"/tmp/cellsB_${qtag}.tsv"){
      edgesRDDB.mapPartitionsWithIndex{ (pid, it) =>
        val n = it.size
        val wkt = cells(pid).wkt
        val r = s"$pid\t$n\t$wkt\n"
        Iterator(r)
      }.collect
    }

    // Getting LDCELs...
    val dcelsA = getLDCELs(edgesRDDA, cells)
    //val nA = dcelsA.count()
    log("Getting LDCELs for A... done!")

    val dcelsB = getLDCELs(edgesRDDB, cells)
    //val nB = dcelsB.count()
    log("Getting LDCELs for B... done!")
    
    if(params.debug()){
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
    }
    
    // Merging DCELs...
    val sdcel = dcelsA.zipPartitions(dcelsB, preservesPartitioning=true){ (iterA, iterB) =>
      val A = iterA.next.map(_.getNexts).flatten.toList
      val B = iterB.next.map(_.getNexts).flatten.toList

      val hedges = merge2(A, B)

      hedges.toIterator
    }
    val nSDcel = sdcel.count()
    log("Merging DCELs... done!")

    if(params.debug()){
      save("/tmp/edgesF.wkt"){
        sdcel.map{ case(h, tag) =>
          val wkt = h.getPolygon.toText

          s"$wkt\t$tag\n"
        }.collect
      }
    }

    if(params.save()){
      sdcel.map{ case(h, tag) =>
          val wkt = h.getPolygon.toText
          s"$wkt"
      }.toDS.write.mode("overwrite").text("gadm/output")
      logger.info("Saving results at gadm/output.")
    }

    // Running overlay operations...
    val faces = sdcel.map{ case(hedge, tag) =>
      val boundary = hedge.getPolygon
      FaceViz(boundary, tag)
    }

    if(params.overlay()){
      val output_path = params.output()
      overlapOp(faces, intersection, s"${output_path}/edgesInt")
      overlapOp(faces, difference,   s"${output_path}/edgesDif")
      overlapOp(faces, union,        s"${output_path}/edgesUni")
      overlapOp(faces, differenceA,  s"${output_path}/edgesDiA")
      overlapOp(faces, differenceB,  s"${output_path}/edgesDiB")
    }

    spark.close
  }  
}
