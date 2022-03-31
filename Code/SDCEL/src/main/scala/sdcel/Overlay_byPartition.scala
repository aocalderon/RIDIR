package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geom.{Polygon, LineString, Point}
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
import edu.ucr.dblab.sdcel.cells.EmptyCellManager2._

import PartitionReader._
import DCELBuilder2.getLDCELs
import DCELMerger2.merge
import DCELOverlay2.overlay

import Utils._
import LocalDCEL.createLocalDCELs
import SingleLabelChecker.checkSingleLabel

object Overlay_byPartition {
  def main(args: Array[String]) = {
    implicit val now = Tick(System.currentTimeMillis)
    // Starting session...
    implicit val params = new Params(args)
    implicit val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()
    import spark.implicits._
    implicit val conf = spark.sparkContext.getConf
    val appId = conf.get("spark.app.id")
    val qtag = params.qtag()
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
    log(s"INFO|tolerance=${settings.tolerance}")
    implicit val model = new PrecisionModel(settings.scale)
    implicit val geofactory = new GeometryFactory(model)

    // Reading the quadtree for partitioning...
    val (quadtree, cells_prime) = readQuadtree[Int](params.quadtree(), params.boundary())
    val partition = params.partition()
    implicit val cells = Map(0 -> Cell(0, "0", cells_prime(partition).mbr))
    log(s"INFO|npartitions=${cells_prime.size}")
    log(s"INFO|partition=${partition}")

    if(params.debug()){
      save{s"/tmp/edgesQ.wkt"}{
        cells_prime.values.map{ cell =>
          val wkt = cell.wkt
          s"$wkt\n"
        }.toList
      }
    }
    log2(s"TIME|start|$qtag")

    // Reading data...
    val edgesRDDA = readEdges_byPartition(params.input1(), "A")
    val edgesRDDB = readEdges_byPartition(params.input2(), "B")
    log2(s"TIME|read|$qtag")

    // Creating local dcel layer A...
    val ldcelA = createLocalDCELs(edgesRDDA, "A")
    if(params.debug()){
      save(s"/tmp/edgesFA${partition}.wkt"){
        ldcelA.mapPartitionsWithIndex{ (pid, it) =>
          it.map{ hedge =>
            s"${hedge._4.toText}\t${hedge._2}\t${pid}\n"
          }.toIterator
        }.collect
      }
      log2(s"TIME|saveL1|$qtag")
    }
    log2(s"TIME|layer1|$qtag")

    // Creating local dcel layer A...
    val ldcelB = createLocalDCELs(edgesRDDB, "B")
    if(params.debug()){
      save(s"/tmp/edgesFB${partition}.wkt"){
        ldcelB.mapPartitionsWithIndex{ (pid, it) =>
          it.map{ hedge =>
            s"${hedge._4.toText}\t${hedge._2}\t${pid}\n"
          }.toIterator
        }.collect
      }
      log2(s"TIME|saveL1|$qtag")
    }
    log2(s"TIME|layer2|$qtag")

    val m = Map.empty[String, EmptyCell]

    val sdcel = overlay(ldcelA, m, ldcelB, m)
    sdcel.count
    log2(s"TIME|overlay|$qtag")

    if(params.debug()){
      save("/tmp/edgesFO.wkt"){
        sdcel.map{ case(l,w) =>
          s"${w.toText}\t$l\t${w.getUserData}\n"
        }.collect
      }
      log2(s"TIME|saveO|$qtag")
    }
    spark.close
  }
}
