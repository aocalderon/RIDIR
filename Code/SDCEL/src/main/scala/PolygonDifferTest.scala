package edu.ucr.dblab.tests

import com.vividsolutions.jts.geom.{Geometry, Polygon, Coordinate}
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.slf4j.{Logger, LoggerFactory}
import edu.ucr.dblab.sdcel.PartitionReader.readQuadtree
import edu.ucr.dblab.sdcel.DCELBuilder2.save
import edu.ucr.dblab.sdcel.quadtree._

object PolygonDifferTest {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]) = {
    // Starting session...
    logger.info("Starting session...")
    val model = new PrecisionModel(1000)
    implicit val geofactory = new GeometryFactory(model)

    implicit val spark = SparkSession.builder()
        .config("spark.serializer", classOf[KryoSerializer].getName)
        .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
        .getOrCreate()
    import spark.implicits._
    logger.info("Starting session... Done!")

    

    spark.close
  }

  def read(input: String)
    (implicit spark: SparkSession, geofactory: GeometryFactory): SpatialRDD[Polygon] = {

    val polygonRaw = spark.read.textFile(input).rdd.zipWithIndex()
      .mapPartitionsWithIndex{ case(index, lines) =>
        val reader = new WKTReader(geofactory)
        lines.flatMap{ case(line, id) =>
          val geom = reader.read(line.replaceAll("\"", ""))
            (0 until geom.getNumGeometries).map{ i =>
              val poly = geom.getGeometryN(i).asInstanceOf[Polygon]
              val shell = geofactory.createPolygon(poly.getExteriorRing.getCoordinates())
              shell.setUserData(i)
              shell
            }
        }.toIterator
      }
    val polygonRDD = new SpatialRDD[Polygon]()
    polygonRDD.setRawSpatialRDD(polygonRaw)
    polygonRDD.analyze()

    polygonRDD
  }
}
