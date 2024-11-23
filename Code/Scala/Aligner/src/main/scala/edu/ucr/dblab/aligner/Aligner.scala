package edu.ucr.dblab.aligner

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom._
import org.slf4j.{Logger, LoggerFactory}

object Aligner {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]): Unit = {
    implicit val params: AParams = new AParams(args)

    implicit val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.driver.memory","35g")
      .config("deploy.moce","client")
      .config("spark.executor.memory","20g")
      .config("spark.memory.offHeap.enabled",true)
      .config("spark.memory.offHeap.size","16g")
      .config("spark.driver.maxResultSize","4g")
      .config("spark.kryoserializer.buffer.max","256m")
      .master(params.master())
      .appName("Aligner").getOrCreate()
    import spark.implicits._

    implicit val G = new GeometryFactory(new PrecisionModel(1e3))
    logger.info(s"START|")

    /////////////////////////////

    GeoTiffReader.getInfo(params.dataset())

    /////////////////////////////

    spark.close()
    logger.info(s"END|")
  }
}

import org.rogach.scallop._
class AParams(args: Seq[String]) extends ScallopConf(args) {

  val dataset: ScallopOption[String] = opt[String] (default = Some("/mnt/Data1/J/fuzzies/FuzzyArroz.tif"))
  val master:  ScallopOption[String] = opt[String] (default = Some("local[3]"))

  verify()
}
