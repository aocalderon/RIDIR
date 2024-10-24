package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geom.{Polygon, LineString}
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.TaskContext
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.slf4j.{Logger, LoggerFactory}
import org.geotools.geometry.jts.GeometryClipper
import edu.ucr.dblab.sdcel.PartitionReader.readQuadtree
import edu.ucr.dblab.sdcel.quadtree._

import Utils.Settings

object OverlapDetector {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]) = {
    // Starting session...
    logger.info("Starting session...")
    val params = new Params(args)
    val model = new PrecisionModel(params.scale())
    implicit val geofactory = new GeometryFactory(model)
    implicit val settings = Settings()

    implicit val spark = SparkSession.builder()
        .config("spark.serializer",classOf[KryoSerializer].getName)
        .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
        .getOrCreate()
    import spark.implicits._
    logger.info("Starting session... Done!")

    // Read polygons...
    logger.info("Reading data...")
    val (quadtree, cells) = readQuadtree[Polygon](params.quadtree(), params.boundary())
    val polyRDDA = read(params.input1())
    val polyRDDB = read(params.input2())
    val partitioner = new QuadTreePartitioner(quadtree)
    //polyRDDA.spatialPartitioning(partitioner)
    //val polygonsA = polyRDDA.spatialPartitionedRDD.rdd.persist()
    //logger.info(s"Polygons A: ${polygonsA.count}")
    polyRDDB.spatialPartitioning(partitioner)
    val polygonsB = polyRDDB.spatialPartitionedRDD.rdd.persist()    
    logger.info(s"Polygons B: ${polygonsB.count}")
    logger.info("Reading data... Done!")

    /*
    val intersA = polygonsA.mapPartitionsWithIndex{ (pid, itA) =>
      val cell = cells(pid)
      val clipper = new GeometryClipper(cell.mbr.getEnvelopeInternal)
      val polygons = itA.map{ a =>
        val clip = clipper.clip(a, true)
        clip.setUserData(a.getUserData)
        clip
      }.toList
      val inters = for{
        p1 <- polygons
        p2 <- polygons
        if p1.getUserData.asInstanceOf[Long] < p2.getUserData.asInstanceOf[Long]
      } yield {
        val inters = p1.intersection(p2)
        (inters, p1, p2)
      }

      inters.filter(_._1.getNumGeometries > 0).map{ case(geom, p1, p2) =>
        val wkt = geom.toText
        val pid1 = p1.getUserData.toString
        val pid2 = p2.getUserData.toString
        s"$wkt\t$pid1\t$pid2"
      }.toIterator
    }
    val nA = intersA.count
    logger.info(s"Overlaps in A: $nA")
    */
    
    val intersB = polygonsB.mapPartitionsWithIndex{ (pid, itB) =>
      val cell = cells(pid)
      val clipper = new GeometryClipper(cell.mbr.getEnvelopeInternal)
      val polygons = itB.map{ a =>
        val clip = clipper.clip(a, true)
        clip.setUserData(a.getUserData)
        clip
      }.toList
      val inters = for{
        p1 <- polygons
        p2 <- polygons
        if p1.getUserData.asInstanceOf[Long] < p2.getUserData.asInstanceOf[Long]
      } yield {
        val inters = p1.intersection(p2)
        (inters, p1, p2)
      }

      inters.filter(_._1.getNumGeometries > 0).map{ case(geom, p1, p2) =>
        val wkt = geom.toText
        val pid1 = p1.getUserData.toString
        val pid2 = p2.getUserData.toString

        s"$wkt\t$pid1\t$pid2"
      }.toIterator
    }
    val nB = intersB.count
    logger.info(s"Overlaps in B: $nB")
    

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
