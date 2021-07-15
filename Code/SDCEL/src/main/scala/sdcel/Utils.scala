package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel}
import com.vividsolutions.jts.geom.{Geometry, Envelope, Coordinate, Polygon, Point}

import org.apache.spark.sql.{SparkSession, Dataset, Row, functions}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import ch.cern.sparkmeasure.TaskMetrics

import org.slf4j.{Logger, LoggerFactory}

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
  
  def log(msg: String)(implicit logger: Logger, settings: Settings): Unit = {
    val now = System.currentTimeMillis
    logger.info(s"${settings.appId}|$msg")
  }

  def log2(msg: String)(implicit prev: Tick, logger: Logger, settings: Settings): Unit = {
    val now = System.currentTimeMillis
    val duration = now - prev.t
    logger.info(s"${settings.appId}|$duration|$msg")
    prev.t = now
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

