package edu.ucr.dblab.aligner

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom._
import org.slf4j.{Logger, LoggerFactory}

import geotrellis.raster._

object Aligner {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  case class RasterName(name: String, raster: Raster[Tile])

  def main(args: Array[String]): Unit = {
    implicit val params: AParams = new AParams(args)

    implicit val spark: SparkSession = SparkSession.builder().master("local[3]")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .appName("Aligner").getOrCreate()

    implicit val G: GeometryFactory = new GeometryFactory(new PrecisionModel(1e3))
    val appId = spark.sparkContext.applicationId
    logger.info(s"START|$appId")

    /////////////////////////////

    val filenames = params.filenames().split(",").map{ _.trim }
    val filenamesRDD = spark.sparkContext.parallelize(filenames, filenames.length)
    val rasters = filenamesRDD.map{ filename =>
      GeoTiffReader.getRaster(filename)
    }

    if( params.debug() ){
      rasters.map( raster => GeoTiffReader.getInfo(raster) )
        .collect
        .foreach{ info => logger.info( info ) }
    }

    val envelope = rasters.map(_.raster.geom.getEnvelopeInternal).reduce{ (a, b) =>
      a.expandToInclude(b)
      a
    }

    if( params.debug() ){
      logger.info(s"\nEnvelope: ${G.toGeometry(envelope).toText}")
    }

    /////////////////////////////

    spark.close()
    logger.info(s"END|$appId")
  }
}

import org.rogach.scallop._
class AParams(args: Seq[String]) extends ScallopConf(args) {

  val filenames: ScallopOption[String] = opt[String] (default = Some("/home/acald013/Datasets/fuzzies/FuzzyArroz.tif, /home/acald013/Datasets/fuzzies/FuzzyCacao.tif"))
  val debug:     ScallopOption[Boolean] = opt[Boolean] (default = Some(false))

  verify()
}
