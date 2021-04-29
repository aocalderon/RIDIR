package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel}
import com.vividsolutions.jts.geom.{Geometry, Envelope, Coordinate, Polygon, Point}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import org.slf4j.{Logger, LoggerFactory}

object Utils {
  //** Implicits
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  //** Case Class
  case class Settings(
    tolerance: Double = 1e-3,
    debug: Boolean = false,
    seed: Long = 42L,
    appId: String = "0",
    storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY_2
  ){
    val scale = 1 / tolerance
  }
  
  def log(msg: String)(implicit logger: Logger, settings: Settings): Unit = {
    logger.info(s"${settings.appId}|$msg")
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

