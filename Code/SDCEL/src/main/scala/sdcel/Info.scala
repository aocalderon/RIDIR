package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geom.Polygon
import com.vividsolutions.jts.geom.Envelope
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.io.WKTReader

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD

import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.enums.GridType.QUADTREE
import org.datasyslab.geospark.spatialOperator.RangeQuery

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

    val input1 = "CA/Debug/CA_faces_sequential3.wkt"
    val input2 = "CA/Debug/CA_faces_parallel.wkt"
    val polys1 = read(input1)
    val polys2 = read(input2)

    val polysA = new SpatialRDD[Polygon]()
    polysA.setRawSpatialRDD(polys1)
    polysA.analyze()
    polysA.spatialPartitioning(QUADTREE, 4000)
    polysA.spatialPartitionedRDD.rdd.cache()

    val polysB = new SpatialRDD[Polygon]()
    polysB.setRawSpatialRDD(polys2)
    polysB.analyze()
    polysB.spatialPartitioning(polysA.getPartitioner)
    polysB.spatialPartitionedRDD.rdd.cache()

    val rangeWKT = "Polygon ((-385653 463891, -385653 379214, -230801 379214, -230801 463891, -385653 463891))"

    val rangeWKT2 = "Polygon ((-234511 -17609, -234511 -51871, -141603 -51871, -141603 -17609, -234511 -17609))"

    val rangeWKT3 = "Polygon ((144971 -426605, 144971 -471189, 205097 -471189, 205097 -426605, 144971 -426605))"

    val reader = new WKTReader(geofactory)
    val rangePoly = reader.read(rangeWKT2).asInstanceOf[Polygon]

    val rangeQueryWindow = rangePoly.getEnvelopeInternal
    val considerBoundaryIntersection = true // Only return gemeotries fully covered by the window
    val usingIndex = false
    val result1 = RangeQuery.SpatialRangeQuery(polysA, rangeQueryWindow, considerBoundaryIntersection, usingIndex)

    val result2 = RangeQuery.SpatialRangeQuery(polysB, rangeQueryWindow, considerBoundaryIntersection, usingIndex)


    save("/tmp/edgesResS.wkt"){
      result1.rdd.map{ p =>
        val wkt = p.toText()
        val label = p.getUserData.toString
        s"$wkt\t$label\n"
      }.collect
    }

    save("/tmp/edgesResP.wkt"){
      result2.rdd.map{ p =>
        val wkt = p.toText()
        val label = p.getUserData.toString
        s"$wkt\t$label\n"
      }.collect
    }

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
