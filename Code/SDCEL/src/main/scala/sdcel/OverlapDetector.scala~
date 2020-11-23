package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geom.{Polygon, LineString}
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.TaskContext
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.slf4j.{Logger, LoggerFactory}
import edu.ucr.dblab.sdcel.quadtree._

object OverlapDetector {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]) = {
    // Starting session...
    logger.info("Starting session...")
    val params = new Params(args)
    val model = new PrecisionModel(params.scale())
    implicit val geofactory = new GeometryFactory(model)

    implicit val spark = SparkSession.builder()
        .config("spark.serializer",classOf[KryoSerializer].getName)
        .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
        .getOrCreate()
    import spark.implicits._
    logger.info("Starting session... Done!")

    spark.close
  }

  def read(input: String)
    (implicit spark: SparkSession, geofactory: GeometryFactory): SpatialRDD[Polygon] = {

    val polygonRaw = spark.read.textFile(input).rdd
      .mapPartitionsWithIndex{ case(index, lines) =>
        val reader = new WKTReader(geofactory)
        lines.flatMap{ line =>
          val geom = reader.read(line.replaceAll("\"", ""))
            (0 until geom.getNumGeometries).map{ i =>
              geom.getGeometryN(i).asInstanceOf[Polygon]
            }
        }.toIterator
      }
    val polygonRDD = new SpatialRDD[Polygon]()
    polygonRDD.setRawSpatialRDD(polygonRaw)
    polygonRDD.analyze()

    polygonRDD
  }
}
