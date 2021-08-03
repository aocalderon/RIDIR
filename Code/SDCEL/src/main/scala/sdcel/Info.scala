package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geom.Polygon
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.io.WKTReader

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD

import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator

import edu.ucr.dblab.sdcel.quadtree._
import Utils._

object Info {
  def main(args: Array[String]) = {
    implicit val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()
    import spark.implicits._
    implicit val now = Tick(System.currentTimeMillis)
    implicit val params = new Params(args)
    val conf = spark.sparkContext.getConf
    val appId = conf.get("spark.app.id")
    implicit val settings = Settings(
      tolerance = params.tolerance(),
      debug = params.debug(),
      local = params.local(),
      persistance = params.persistance() match {
        case 0 => StorageLevel.NONE
        case 1 => StorageLevel.MEMORY_ONLY
        case 2 => StorageLevel.MEMORY_ONLY_SER
        case 3 => StorageLevel.MEMORY_ONLY_2
        case 4 => StorageLevel.MEMORY_ONLY_SER_2
      },
      appId = appId
    )
    val command = System.getProperty("sun.java.command")
    log(command)

    val model = new PrecisionModel(settings.scale)
    implicit val geofactory = new GeometryFactory(model)

    val input1 = "CA/Debug/CA_faces_sequential2.wkt"

    val test = "POLYGON ((-366947.4126000000 307585.0536000000 -367812.9856000000 306012.3385000000, -367812.9856000000 306012.3385000000 -367862.6649000000 305922.0394000000, -367862.6649000000 305922.0394000000 -366947.4126000000 307585.0536000000))"
    val test2 = "POLYGON ((-366947.4126000000 307585.0536000000, -367812.9856000000 306012.3385000000, -367862.6649000000 305922.0394000000, -366947.4126000000 307585.0536000000))"
    val reader = new WKTReader(geofactory)
    val poly = reader.read(test2)
    println(poly.toText())

    //val polys = read(input1)
    //polys.take(1).foreach(println)

    spark.close
  }

  def read(input: String)
    (implicit spark: SparkSession, geofactory: GeometryFactory): RDD[Polygon] = {

    val polygonRaw = spark.read.option("header", "false").option("delimiter", "\t").
      csv(input).rdd
      .mapPartitions{ lines =>
        val reader = new WKTReader(geofactory)
        lines.map{ line =>
          val wkt = line.getString(0)
          println(wkt)
          val polygon_prime = reader.read(wkt).asInstanceOf[Polygon]
          val polygon = getExteriorPolygon(polygon_prime)
          val id = line.getString(1)
          polygon.setUserData(id)
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
