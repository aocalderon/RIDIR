package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geom.{Geometry, Polygon, LinearRing}
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.slf4j.{Logger, LoggerFactory}

import collection.JavaConverters._
import edu.ucr.dblab.sdcel.DCELBuilder2.save

object PolygonChecker {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")
 
  val SCALE = 10e13

  case class Info(id: Long, code: String){
    override def toString = s"$id\t$code"
  }

  def main(args: Array[String]): Unit = {
    logger.info("Starting session.")
    val model = new PrecisionModel(SCALE)
    implicit val geofactory = new GeometryFactory(model)

    implicit val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    readAndSave("file:///home/acald013/Datasets/gadm/raw/level1.tsv", "level1")
    
    val polygons = read("gadm/raw/level1")
    save("/tmp/edgesUSA.wkt"){
      polygons.filter(_.getUserData.asInstanceOf[Info].code == "USA").map{ polygon =>
        val wkt = polygon.toText
        val info = polygon.getUserData.toString

        s"$wkt\t$info\n"
      }.collect
    }

    spark.close
  }

  def readAndSave(input: String, output: String)
    (implicit spark: SparkSession, geofactory: GeometryFactory): Unit = {

    import spark.implicits._
    val polygonRaw0 = spark.read
      .option("header", "true")
      .option("delimiter", "\t")
      .option("quote", "\"")
      .csv(input)
    polygonRaw0.rdd.filter(_.size < 2).foreach(println)

    val polygonRaw = polygonRaw0.rdd.mapPartitions{ lines =>
        val reader = new WKTReader(geofactory)
        lines.flatMap{ line =>
          val wkt = line.getString(0)
          val geom = reader.read(wkt)
          val code = line.getString(1)

          (0 until geom.getNumGeometries).map{ i =>
            val poly = geom.getGeometryN(i).asInstanceOf[Polygon]
            val shell = getExteriorPolygon(poly)
            (shell.toText, code)
          }
        }
      }.zipWithUniqueId.map{ case(row, id) =>
          val wkt = row._1
          val code = row._2
          s"$wkt\t$id\t$code"
      }.cache
    val n = polygonRaw.count

    polygonRaw.toDF.write.mode(SaveMode.Overwrite).text(s"gadm/raw/$output")
    logger.info(s"$n polygons have been saved to gadm/raw/$output.")
  }

  def read(input: String)
    (implicit spark: SparkSession, geofactory: GeometryFactory): RDD[Polygon] = {

    val polygonRaw = spark.read.textFile(input).rdd
      .mapPartitions{ lines =>
        val reader = new WKTReader(geofactory)
        lines.map{ line =>
          val arr = line.split("\t")
          val wkt = arr(0)
          val polygon = reader.read(wkt).asInstanceOf[Polygon]
          val id = arr(1).toLong
          val code = arr(2)
          polygon.setUserData(Info(id, code))
          polygon
        }
      }

    polygonRaw
  }

  def getExteriorPolygon(polygon: Polygon)
    (implicit geofactory: GeometryFactory): Polygon = {
    val poly = geofactory.createPolygon(polygon.getExteriorRing.getCoordinateSequence)
    poly.setUserData(polygon.getUserData.toString())
    poly
  }
}