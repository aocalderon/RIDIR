package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geom.{Geometry, Polygon}
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.enums.GridType
import org.slf4j.{Logger, LoggerFactory}
import edu.ucr.dblab.sdcel.PartitionReader.readQuadtree
import edu.ucr.dblab.sdcel.DCELBuilder2.save
import edu.ucr.dblab.sdcel.quadtree._

object PolygonDiffer {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]) = {
    // Starting session...
    logger.info("Starting session...")
    val params = new Params(args)
    val model = new PrecisionModel(params.scale())
    implicit val geofactory = new GeometryFactory(model)

    implicit val spark = SparkSession.builder()
        .config("spark.serializer", classOf[KryoSerializer].getName)
        .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
        .getOrCreate()
    import spark.implicits._
    logger.info("Starting session... Done!")

    // Read polygons and quadtree...
    logger.info("Reading data...")
    val polyRDD = read(params.input1())
    polyRDD.spatialPartitioning(GridType.QUADTREE, 256)
    val polygons = polyRDD.spatialPartitionedRDD.rdd
    polygons.persist()
    logger.info(s"Polygons: ${polygons.count}")
    logger.info("Reading data... Done!")
    /*
    polyRDD.getRawSpatialRDD.rdd.map{ poly =>
      val wkt = poly.toText
      val id = poly.getUserData.asInstanceOf[Long]

      s"$wkt\t$id"
    }.toDS.write.mode(SaveMode.Overwrite).text("gadm/level2raw")
     */

    val differs = polygons.mapPartitionsWithIndex{ (pid, itPolygons) =>
      val polygons = itPolygons.toList
      val overlaps = for{
        p1 <- polygons
        p2 <- polygons
        if p1.relate(p2, "2********") &&
        p1.getUserData.asInstanceOf[Long] < p2.getUserData.asInstanceOf[Long]
      } yield {
        (p1, p2)
      }

      overlaps.map{ case(a, b) =>
        val wkt1 = a.toText
        val id1 = a.getUserData.toString
        val wkt2 = b.toText
        val id2 = b.getUserData.toString

        s"$wkt1\t$wkt2\t$id1\t$id2"
      }.toIterator
    }.persist
    val n = differs.count
    logger.info(s"Results: $n")

    differs.toDS.write.mode(SaveMode.Overwrite).text("gadm/overlaps")

    spark.close
  }

  def read(input: String)
    (implicit spark: SparkSession, geofactory: GeometryFactory): SpatialRDD[Polygon] = {

    val polygonRaw = spark.read.textFile(input).rdd
      .mapPartitionsWithIndex{ case(index, lines) =>
        val reader = new WKTReader(geofactory)
        lines.flatMap{ line =>
          val arr = line.split("\t")
          val wkt = arr(0).replaceAll("\"", "")
          val geom = reader.read(wkt)
          val id = arr(1).toLong

          (0 until geom.getNumGeometries).map{ i =>
            val poly = geom.getGeometryN(i).asInstanceOf[Polygon]
            val shell = geofactory.createPolygon(poly.getExteriorRing.getCoordinates)
            shell.setUserData(id)
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
