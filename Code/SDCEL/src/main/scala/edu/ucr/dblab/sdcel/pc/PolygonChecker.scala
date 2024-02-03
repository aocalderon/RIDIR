package edu.ucr.dblab.sdcel.pc

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, Polygon, PrecisionModel}
import com.vividsolutions.jts.io.WKTReader
import edu.ucr.dblab.sdcel.Utils.save
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.SpatialRDD

import scala.collection.JavaConverters._

object PolygonChecker {

  private def readPolygons(input: String)(implicit spark: SparkSession, G: GeometryFactory): RDD[Polygon] = {
    spark.read.option("header", value = false).option("delimiter", "\t").csv(input)
      .rdd
      .mapPartitions { rows =>
        val reader = new WKTReader(G)
        rows.map { row =>
          val geom = reader.read(row.getString(0)).asInstanceOf[Polygon]
          geom.setUserData(row.getString(1).toLong)
          geom
        }
      }
  }
  private def formatRDD(polygons: RDD[Polygon]): Array[String] = {
    polygons.map { polygon =>
      val wkt = polygon.toText
      val id = getPolygonId(polygon)

      s"$wkt\t$id\n"
    }.collect
  }

  private def getPolygonId(p: Polygon): Long = p.getUserData.asInstanceOf[Long]

  private def readOriginalPolygons(input: String, output: String)(implicit spark: SparkSession, G: GeometryFactory): Unit = {

    val data = spark.read.option("header", value = false).csv(input)
    println(s"Number of original polygons: ${data.count}")

    val null_point = G.createPoint(new Coordinate(0.0, 0.0))
    val null_envelope = null_point.getEnvelopeInternal
    null_envelope.expandBy(1.0)
    val null_geom = G.toGeometry(null_envelope)

    val geometries = data.rdd.mapPartitions{ lines =>
      val reader: WKTReader = new WKTReader(G)
      lines.map{ row =>
        try{
          val id = row.getString(0).toLong
          val wkt = row.getString(1)
          val geom = reader.read(wkt)
          geom.setUserData(id)
          geom
        } catch {
          case e1: java.lang.Exception =>
            println(s"[Error 1] ${row.getString(0)}: ${row.getString(1)} (${e1.getMessage})")
            null_geom
          case e2: com.vividsolutions.jts.io.ParseException =>
            println(s"[Error 2] ${row.getString(0)}: ${row.getString(1)} (${e2.getMessage})")
            null_geom
        }
      }
    }.filter(_.getCentroid != null_point.getCentroid).zipWithUniqueId().map{ case(geom, index) =>
      geom.setUserData(index)
      geom
    }

    val result = geometries.map{ geom =>
      val wkt = geom.toText
      val  id = geom.getUserData.asInstanceOf[Long]

      s"$wkt\t$id\n"
    }

    save(output){
      result.collect()
    }
  }

  private def extractValidPolygons(input: String, output: String)(implicit spark: SparkSession, G: GeometryFactory): Unit = {

    val polygons = spark.read.option("header", value = false).option("delimiter", "\t")
      .csv(input)
      .rdd
      .mapPartitions{ rows =>
        val reader = new WKTReader(G)
        rows.map{ row =>
          val wkt = row.getString(0)
          val  id = row.getString(1).toLong

          val poly = reader.read(wkt).asInstanceOf[Polygon]
          poly.setUserData(id)
          poly
        }
      }

    save("/tmp/edgesInvalids.wkt"){
      val invalids = polygons.filter(!_.isValid).cache
      println(s"Number of invalid polygons: ${invalids.count()}")
      formatRDD( invalids )
    }
    save(output){
      formatRDD( polygons.filter(_.isValid) )
    }
  }

  private def extractOverlappedPolygons(input: String, output: String)(implicit spark: SparkSession, G: GeometryFactory): Unit = {

    val polygons = readPolygons(input)
    val polygonsRDD = new SpatialRDD[Polygon]()
    polygonsRDD.setRawSpatialRDD(polygons)
    polygonsRDD.analyze()
    polygonsRDD.spatialPartitioning(GridType.QUADTREE, 64)

    case class OverlapInfo(pid: Long, wkt: String, overlaps: List[Long]){
      def toText: String = s"$wkt\t$pid\t${overlaps.mkString(", ")}\n"

    }
    val overlaps = JoinQuery.SpatialJoinQuery(polygonsRDD, polygonsRDD, false, true)
      .rdd
      .map { case (a, bb) =>
        val  id = getPolygonId(a)
        val wkt = a.toText
        val ove = bb.asScala.filter{ b => a.overlaps(b) }.map{ b => getPolygonId(b) }.toList

        OverlapInfo(id, wkt, ove)
      }.filter( _.overlaps.nonEmpty ).cache

    println(s"Number of overlapped polygons: ${overlaps.count}")

    save("/tmp/edgesOverlaps.wkt"){
      overlaps.map(_.toText).collect()
    }

    val overlapped_set = overlaps.map(_.pid).collect().sorted.toList
    val clean = polygons.filter{ polygon => !overlapped_set.contains( getPolygonId(polygon) ) }

    save(output){
      formatRDD( clean )
    }
  }

  def main(args: Array[String]): Unit = {
    val home = sys.env("HOME")
    val tolerance: Double = 1e-6
    implicit val G: GeometryFactory = new GeometryFactory(new PrecisionModel(1/tolerance))
    implicit val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()

    readOriginalPolygons(s"$home/Datasets/polygons.csv", s"$home/Datasets/PolygonsDDCEL.wkt")

    extractValidPolygons(s"$home/Datasets/PolygonsDDCEL.wkt", s"$home/Datasets/PolygonsDDCEL_valids.wkt")

    extractOverlappedPolygons(s"$home/Datasets/PolygonsDDCEL_valids.wkt", s"$home/Datasets/PolygonsDDCEL_valids_no-overlap.wkt")

    spark.close
  }
}
