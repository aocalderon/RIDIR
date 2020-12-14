package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geom.{Geometry, Polygon}
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions._
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.enums.GridType

import collection.JavaConverters._
import edu.ucr.dblab.sdcel.DCELBuilder2.save

object OverlapsReader {

  case class Overlap(wkt1: String, wkt2: String, id1: Long, id2:Long)

  def main(args: Array[String]): Unit = {
    val params = new Params(args)
    val model = new PrecisionModel(params.scale())
    implicit val geofactory = new GeometryFactory(model)

    implicit val spark = SparkSession.builder()
        .config("spark.serializer", classOf[KryoSerializer].getName)
        .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
        .getOrCreate()
    import spark.implicits._

    val data = spark.read.textFile("gadm/overlaps").persist

    val polys = data.map{ p =>
      val arr = p.split("\t")
      Overlap(arr(0), arr(1), arr(2).toLong, arr(3).toLong)
    }.persist

    val groups = polys.groupBy($"id1", $"wkt1").agg(collect_list($"wkt2").as("multipoly"))
      .filter($"id1" === 46)

    val differs = groups.rdd.map{ g =>
      val reader = new WKTReader(geofactory)
      val id = g.getLong(0)
      val left = reader.read(g.getString(1))
      val polys = g.getList[String](2).asScala.distinct.map{ p =>
        reader.read(p).asInstanceOf[Polygon]
      }.toArray
      val differ = try{
        var d = left.difference(polys(0))
        val n = polys.size
        var i = 1
        while(i < n){
          d = d.difference(polys(i))
          i = i + 1
        }
        d
      } catch {
        case e: com.vividsolutions.jts.geom.TopologyException => {
          println(e.getMessage)
          save(s"/tmp/edgesL$id.wkt")(List(left.toText))
          save(s"/tmp/edgesR$id.wkt")(polys.map(_.toText + "\n"))
          left
        }
      }
      (differ, id)
    }

    save("/tmp/edgesDiffer.wkt"){
      differs.map{ differ =>
        val wkt = differ._1.toText()
        val id = differ._2

        s"$wkt\t$id\t${differ._1.getGeometryType}\n"
      }.collect
    }
  }
}
