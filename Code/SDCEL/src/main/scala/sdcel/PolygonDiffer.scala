package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geom.{Geometry, Polygon}
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

object PolygonDiffer {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  case class Info(id: Long, desc: String, geomtype: String){
    override def toString(): String =  s"$id\t$desc\t$geomtype"
  }

  def setInfo(p: Geometry, id: Long, desc: String): Geometry = {
    p.setUserData(Info(id, desc, p.getGeometryType))
    p
  }

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
    val (quadtree, cells) = readQuadtree[Polygon](params.quadtree(), params.boundary())
    save("/tmp/Cells.wkt"){
      cells.values.map{_.wkt + "\n"}.toList
    }
    val polyRDD = read(params.input1())
    val partitioner = new QuadTreePartitioner(quadtree)
    polyRDD.spatialPartitioning(partitioner)
    val polygons = polyRDD.spatialPartitionedRDD.rdd
      //.filter{ p =>
      //  p.getUserData.asInstanceOf[Info].id < 1000
      //}
    polygons.persist()
    logger.info(s"Polygons: ${polygons.count}")
    logger.info("Reading data... Done!")
    
    val differs = polygons.mapPartitionsWithIndex{ (pid, itPolygons) =>
      val polygons = itPolygons.toList
      val overlaps = for{
        p1 <- polygons
        p2 <- polygons
        if p1.intersects(p2) &&
        p1.getUserData.asInstanceOf[Info].id < p2.getUserData.asInstanceOf[Info].id
      } yield {
        (p1, p2)
      }

      overlaps.map{ case(a, b) =>
        val wkt1 = a.toText
        val id1 = a.getUserData.toString
        val wkt2 = b.toText
        val id2 = b.getUserData.toString

        s"$wkt1\t$wkt2\t$id1\t$id2\n"
      }.toIterator
    }.persist
    val n = differs.count
    logger.info(s"Results: $n")

    save("/tmp/edgesSample.wkt"){
      differs.take(100)
    }
    differs.toDS.write.mode(SaveMode.Overwrite).text("gadm/overlaps")

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
              shell.setUserData(Info(id, "", shell.getGeometryType))
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
