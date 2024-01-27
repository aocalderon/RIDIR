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
          geom.setUserData(row.getString(1).toInt)
          geom
        }
      }
  }
  def readOriginalPolygons(input: String, output: String)(implicit spark: SparkSession, G: GeometryFactory): Unit = {
    import spark.implicits._

    val data = spark.read.option("header", value = false).csv(input)
    println(data.count)

    val null_point = G.createPoint(new Coordinate(0.0, 0.0))
    val null_envelope = null_point.getEnvelopeInternal
    null_envelope.expandBy(1.0)
    val null_geom = G.toGeometry(null_envelope)

    val geometries = data.rdd.zipWithUniqueId().mapPartitions{ lines =>
      val reader: WKTReader = new WKTReader(G)
      lines.map{ case(row, index) =>
        try{
          val id = row.getString(0).toInt
          val wkt = row.getString(1)
          val geom = reader.read(wkt)
          geom.setUserData(id)
          geom
        } catch {
          case e1: java.lang.Exception =>
            println(s"$index: ${row.getString(0)} ${row.getString(1)}")
            println(s"Error1: ${e1.getMessage}")
            null_geom
          case e2: com.vividsolutions.jts.io.ParseException =>
            println(s"$index: ${row.getString(0)} ${row.getString(1)}")
            println(s"Error2: ${e2.getMessage}")
            null_geom
        }
      }
    }.filter(_.getCentroid != null_point.getCentroid)

    geometries.take(10).foreach(println)

    val result = geometries.map{ geom =>
      val wkt = geom.toText
      val  id = geom.getUserData.toString

      s"$wkt\t$id\n"
    }

    result.toDF.show()

    save(output){
      result.collect()
    }
  }

  def extractValidPoligons(input: String, output: String)(implicit spark: SparkSession, G: GeometryFactory): Unit = {
    import spark.implicits._

    val polygons = spark.read.option("header", value = false).option("delimiter", "\t")
      .csv(input)
      .rdd
      .mapPartitions{ rows =>
        val reader = new WKTReader(G)
        rows.map{ row =>
          val wkt = row.getString(0)
          val  id = row.getString(1).toInt

          val poly = reader.read(wkt).asInstanceOf[Polygon]
          poly.setUserData(id)
          poly
        }
      }.filter(_.isValid)
    save(output){
      polygons.map{ poly =>
        val wkt = poly.toText
        val  id = poly.getUserData.asInstanceOf[Int]

        s"$wkt\t$id\n"
      }.collect
    }
  }
  def main(args: Array[String]): Unit = {
    val home = sys.env("HOME")
    implicit val G: GeometryFactory = new GeometryFactory(new PrecisionModel(100000))
    implicit val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()

    //readOriginalPolygons(s"$home/Downloads/polygons.csv", s"$home/Datasets/PolygonsDDCEL.wkt")
    //extractValidPoligons(s"$home/Datasets/PolygonsDDCEL.wkt", s"$home/Datasets/PolygonsDDCEL_valids.wkt")

    val polygons = readPolygons(s"$home/Datasets/PolygonsDDCEL_valids.wkt")
    val polygonsRDD = new SpatialRDD[Polygon]()
    polygonsRDD.setRawSpatialRDD(polygons)
    polygonsRDD.analyze()
    polygonsRDD.spatialPartitioning(GridType.QUADTREE, 64)

    val join_results = JoinQuery.SpatialJoinQueryFlat(polygonsRDD, polygonsRDD, false, true)

    val overlapped_ids = join_results.rdd.map { case (a, b) =>
      val aId = a.getUserData.asInstanceOf[Int]
      val bId = b.getUserData.asInstanceOf[Int]
      val ove = a.overlaps(b)
      (aId, bId, ove)
    }.filter(_._3).filter { case (a, b, _) => a < b }.flatMap{ case(a, b, _) => List(a, b) }.distinct()

    val o = overlapped_ids.count
    println(s"Number of polygons involved: $o")

    import spark.implicits._
    val wkts = polygons.map{ poly =>
      val wkt = poly.toText
      val  id = poly.getUserData.asInstanceOf[Int]

      (id, wkt)
    }.toDF("id", "wkt")

    val ids = overlapped_ids.toDF("id")

    val overlaps = wkts.join(ids, wkts("id") === ids("id")).select($"wkt")

    overlaps.show()

    save("/tmp/edgesO.wkt"){
      overlaps.map(_.getString(0) + "\n").collectAsList().asScala
    }

    spark.close
  }
}
