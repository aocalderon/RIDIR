package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geom.{LineString, Point}
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
    implicit val now = Tick(System.currentTimeMillis)
    // Starting session...
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
    log2(s"COMMAND|$command")

    val model = new PrecisionModel(settings.scale)
    implicit val geofactory = new GeometryFactory(model)

    val (quadtree, cells) = readQuadtree[Int](params.quadtree(), params.boundary())

    val qtag = params.qtag()
    if(params.debug()){
      log(s"INFO|scale=${settings.scale}")
      save{s"/tmp/edgesCells_${qtag}.wkt"}{
        cells.values.map{ cell =>
          val wkt = envelope2polygon(cell.mbr.getEnvelopeInternal).toText
          val id = cell.id
          val lineage = cell.lineage
          s"$wkt\t$id\t$lineage\n"
        }.toList
      }
    }
    log(s"INFO|npartitions=${cells.size}")
    log2("TIME|start")

    // Reading data...
    val edgesRDDA = readEdges(params.input1(), quadtree, "A").cache
    val nEdgesRDDA = edgesRDDA.count()
    log(s"INFO|nEdgesA=${nEdgesRDDA}")

    val edgesRDDB = readEdges(params.input2(), quadtree, "B").cache
    val nEdgesRDDB = edgesRDDB.count()
    log(s"INFO|nEdgesB=${nEdgesRDDB}")
    log2("TIME|read")

    if(params.debug()){
      save(s"/tmp/edgesCA_${qtag}.wkt"){
        edgesRDDA.mapPartitionsWithIndex{ (pid, it) =>
          val edges = it.toList
          val cell = cells(pid).mbr
          val non_empty = edges.exists{ _.intersects(cell) }
          val n = edges.size
          val wkt = cells(pid).wkt
          val r = s"$wkt\t$pid\t$n\t$non_empty\n"
          Iterator(r)
        }.collect
      }
      save(s"/tmp/edgesCB_${qtag}.wkt"){
        edgesRDDB.mapPartitionsWithIndex{ (pid, it) =>
          val edges = it.toList
          val cell = cells(pid).mbr
          val empty = edges.exists{ _.intersects(cell) }
          val n = edges.size
          val wkt = cells(pid).wkt
          val r = s"$pid\t$n\t$wkt\n"
          Iterator(r)
        }.collect
      }
    }

    // Getting LDCELs...
    val dcelsA = getLDCELs(edgesRDDA, cells)
    if(params.notmerge()){
      val nA = dcelsA.count()
      log(s"INFO|nA=$nA")
    }
    log2("TIME|ldcelA")

    val dcelsB = getLDCELs(edgesRDDB, cells)
    if(params.notmerge()){
      val nB = dcelsA.count()
      log(s"INFO|nB=$nB")
    }
    log2("TIME|ldcelB")

    
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

    if(!params.notmerge()){
      // Merging DCELs...
      val sdcel = dcelsA
        .zipPartitions(dcelsB, preservesPartitioning=true){ (iterA, iterB) =>

          val A = iterA.next.map(_.getNexts).flatten.toList
          val B = iterB.next.map(_.getNexts).flatten.toList
          val hedges = merge2(A, B)

          hedges.toIterator
      }
      val nSDcel = sdcel.count()
      log2("TIME|merge")

      // *************************************
      // * START: Dealing with empty cells...
      // *************************************
      val (ne, e) = sdcel.mapPartitionsWithIndex{ (pid, it) =>
        val faces = it.map(_._1.getPolygon).toList
        val cell = cells(pid).mbr
        val non_empty = faces.exists{ _.intersects(cell) }
        val r = (pid, non_empty)
        Iterator(r)
      }.collect().partition{ case(pid, non_empty) => non_empty }
      val non_empties = ne.map(_._1).toList
      val empties = e.map(_._1).toList
      println(s"Non empties set: ${non_empties.mkString(" ")}")
      println(s"Empties set: ${empties.mkString(" ")}")
      import edu.ucr.dblab.sdcel.cells.EmptyCellManager._
      val r_prime = solve(quadtree, cells, non_empties, empties)
      r_prime.foreach(println)
      // Getting polygon IDs from known partitions...
      val r = updatePolygonIds(r_prime, sdcel)
      val str = r.map{_.toString}.mkString("\n")
      println(s"r:\n$str")
      val sdcel_prime = fixEmptyCells(r, sdcel, cells)
      // ***********************************
      // * END: Dealing with empty cells...
      // ***********************************

      if(params.debug()){
        save("/tmp/edgesF.wkt"){
          sdcel.map{ case(h, tag) =>
            val wkt = h.getPolygon.toText

            s"$wkt\t$tag\n"
          }.collect
        }
        save("/tmp/edgesF_prime.wkt"){
          sdcel_prime.map{ case(h, tag) =>
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
    }

    spark.close
  }  
}
