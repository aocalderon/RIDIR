package edu.ucr.dblab.sdcel.pc

import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel}
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.sql.SparkSession

object PolygonChecker {
  def main(args: Array[String]): Unit = {
    val G: GeometryFactory = new GeometryFactory(new PrecisionModel(100000))
    implicit val spark: SparkSession = SparkSession.builder().master("local[8]").getOrCreate()
    import spark.implicits._

    val data = spark.read.option("header", value = false).csv("/home/acald013/Downloads/polygons.csv")
    println(data.count)

    val geometries = data.rdd.zipWithUniqueId().mapPartitions{ lines =>
      val reader: WKTReader = new WKTReader(G)
      lines.map{ case(row, index) =>
        val id = try{
          row.getString(0).toInt
        } catch {
          case e1: java.lang.Exception => println(s"${row.getString(1)}-> ${e1.getMessage}")
          case e2: com.vividsolutions.jts.io.ParseException => println(s"${row.getString(1)} -> ${e2.getMessage}")
        } finally {
          -99999
        }
        val wkt = row.getString(1)
        val geom = reader.read(wkt)
        geom.setUserData(id)
        geom
      }
    }

    geometries.take(10).foreach(println)

    val result = geometries.map{ geom =>
      val wkt = geom.toText
      val  id = geom.getUserData.asInstanceOf[Int]

      s"$wkt\t$id\n"
    }

    result.toDF.show()

    edu.ucr.dblab.sdcel.Utils.save("/tmp/edgesP.wkt"){
      result.collect()
    }

    spark.close
  }
}
