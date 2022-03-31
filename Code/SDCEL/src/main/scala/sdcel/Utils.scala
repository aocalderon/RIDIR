package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geom.{ GeometryFactory, PrecisionModel }
import com.vividsolutions.jts.geom.{ Geometry, Envelope, Coordinate }
import com.vividsolutions.jts.geom.{ Polygon, Point }

import org.apache.spark.sql.{ SparkSession, Dataset, Row, functions }
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{ TaskContext, SparkEnv }

import ch.cern.sparkmeasure.TaskMetrics
import org.slf4j.{ Logger, LoggerFactory }

import edu.ucr.dblab.sdcel.geometries.{ Half_edge, Cell }
import edu.ucr.dblab.sdcel.cells.EmptyCellManager2.{ getFaces, EmptyCell }

object Utils {
  //** Implicits
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  //** Case Class
  case class Timestamp(t: Long)
  case class Tick(var t: Long)
  case class Settings(
    tolerance: Double = 1e-3,
    debug: Boolean = false,
    local: Boolean = false,
    seed: Long = 42L,
    appId: String = "0",
    persistance: StorageLevel = StorageLevel.MEMORY_ONLY_2
  ){
    val scale = 1 / tolerance
  }
  
  def log(msg: String)(implicit settings: Settings): Unit = {
    val now = System.currentTimeMillis
    logger.info(s"${settings.appId}|$msg")
  }

  def log2(msg: String)(implicit prev: Tick, settings: Settings): Unit = {
    val now = System.currentTimeMillis
    val duration = now - prev.t
    
    logger.info(s"${settings.appId}|$duration|$msg")
    prev.t = now
  }

  def getPartitionLocation(pid: Long)(implicit settings: Settings): String = {
    val eid  = SparkEnv.get.executorId
    val host = java.net.InetAddress.getLocalHost().getHostName()
    logger.info(s"Partition $pid at executor $eid in $host...")
    s"${settings.appId}|${host}:${eid}"
  }

  def saveSDCEL(output: String, 
    sdcel: RDD[(Half_edge, String, Envelope, Polygon)],
    m: Map[String, EmptyCell])
    (implicit geofactory: GeometryFactory, cells: Map[Int, Cell], settings: Settings) = {

    case class Face(h: Half_edge, l: String, e: Envelope, p: Polygon){
      val id = h.id

      override def toString: String = s"$id\t$l\t$e\t$p"

      def toRecord(pid: Long = -1L): String = s"$p\t$id\t$l\t$pid"
    }
    case class Data(pid: Int, hedges: List[Half_edge],
      faces: List[Face] = List.empty[Face]){

      override def toString: String = {
        "Partition:\n" +
        s"$pid\n" +
        s"Half Edges:\n" +
        hedges.map{ h => 
          val par  = h.params
          val wkt  = h.wkt
          val id   = h.id
          val prev = h.prev.id
          val next = h.next.id
          val twin = if( h.twin != null) h.twin.id else null

          s"$wkt\t$id\t$par\t$prev\t$next\t$twin"
        }.mkString("\n") +
        "\n" +
        s"Faces:\n" +
        faces.mkString("\n") +
        "\n" 
      }

      def getHalf_edges: List[String] = {
        hedges.map{ h => 
          val par  = h.params
          val wkt  = h.wkt
          val id   = h.id
          val prev = h.prev.id
          val next = h.next.id
          val twin = if( h.twin != null) h.twin.id else null

          s"$wkt\t$pid\t$id\t$par\t$prev\t$next\t$twin"
        }
      }
    }

    sdcel.mapPartitionsWithIndex{ (pid, it) =>
      val faces = getFaces(it, cells(pid), m).map{ case(h,l,e,p) => Face(h,l,e,p) }
      val hedges = faces.flatMap{_.h.getNexts}

      Data(pid, hedges).getHalf_edges.toIterator
    }.saveAsTextFile(output + "/hedges")
    log(s"INFO|SDCEL half edges saved at ${output}/hedges")

    sdcel.mapPartitionsWithIndex{ (pid, it) =>
      val faces = getFaces(it, cells(pid), m)
        .map{ case(h,l,e,p) => Face(h,l,e,p).toRecord(pid) }

      faces.toIterator
    }.saveAsTextFile(output + "/faces")
    log(s"INFO|SDCEL faces saved at ${output}/faces")
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

  def clocktime = System.currentTimeMillis()

  def getPhaseMetrics(metrics: TaskMetrics, phaseName: String)(implicit settings: Settings): Dataset[Row] = {
    metrics.createTaskMetricsDF()
      .withColumn("appId", functions.lit(settings.appId))
      .withColumn("phaseName", functions.lit(phaseName))
      .select("host", "index", "launchTime", "finishTime", "duration", "appId", "phaseName")
      .orderBy("launchTime")
  }

  def round(number: Double)(implicit geofactory: GeometryFactory): Double = {
    val scale = geofactory.getPrecisionModel.getScale
    Math.round(number * scale) / scale
  }

  import Numeric.Implicits._
  def mean[T: Numeric](xs: Iterable[T]): Double = xs.sum.toDouble / xs.size
  def variance[T: Numeric](xs: Iterable[T]): Double = {
    val avg = mean(xs)
    xs.map(_.toDouble).map(a => math.pow(a - avg, 2)).sum / xs.size
  }
  def stdDev[T: Numeric](xs: Iterable[T]): Double = math.sqrt(variance(xs))
}

