package edu.ucr.dblab.sdcel.pc

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, Polygon, PrecisionModel}
import com.vividsolutions.jts.io.WKTReader
import edu.ucr.dblab.sdcel.Utils.save
import org.apache.spark.sql.SparkSession

object PolygonChecker {

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
  def main(args: Array[String]): Unit = {
    implicit val G: GeometryFactory = new GeometryFactory(new PrecisionModel(100000))
    implicit val spark: SparkSession = SparkSession.builder().master("local[1]").getOrCreate()
    import spark.implicits._

    //readOriginalPolygons("/home/acald013/Downloads/polygons.csv", "/home/acald013/Datasets/PolygonsDDCEL.wkt")

    val polygons = spark.read.option("header", false).option("delimiter", "\t")
      .csv("/home/acald013/Datasets/PolygonsDDCEL.wkt")
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
      }
    save("/tmp/edgesP2.wkt"){
      polygons.map{ poly =>
        val wkt = poly.toText
        val  id = poly.getUserData.asInstanceOf[Int]

        val isValid  = poly.isValid
        val isSimple = poly.isSimple

        s"$wkt\t$id\t$isSimple\t$isValid\n"
      }.collect
    }

    spark.close
  }
}
