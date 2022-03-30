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

import Utils._
import LocalDCEL.createLocalDCELs
import SingleLabelChecker.checkSingleLabel

object SDCEL2_byPartition {
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
    val cells = Map(0 -> Cell(0, "0", cells_prime(partition).mbr))
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
    val nEdgesRDDA = edgesRDDA.count()
    log2(s"TIME|read|$qtag")
    log(s"INFO|nEdgesA=${nEdgesRDDA}")
    if(params.debug()){
      save{s"/tmp/edgesE${partition}.wkt"}{
        edgesRDDA.map{ edge =>
          val wkt = edge.toText
          val dat = edge.getUserData.toString
          s"$wkt\t$dat\n"
        }.collect
      }
    }

    // Creating local dcel layer A...
    val ldcelA0 = createLocalDCELs(edgesRDDA, cells)
    log2(s"TIME|layer1|$qtag")

    if(params.debug()){
      save(s"/tmp/edgesF${partition}.wkt"){
        ldcelA0.mapPartitionsWithIndex{ (pid, it) =>
          it.map{ hedge =>
            s"${hedge._4.toText}\t${hedge._2}\t${pid}\n"
          }.toIterator
        }.collect
      }
      log2(s"TIME|saveL1|$qtag")
    }

    ldcelA0.count
    spark.close
  }
}
