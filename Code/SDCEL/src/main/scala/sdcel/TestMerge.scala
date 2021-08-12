package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geom.{Polygon, LineString}
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

import edu.ucr.dblab.sdcel.DCELOverlay2.mergeLines

object TestMerge {
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

    val input = "file:///home/acald013/RIDIR/Datasets/Debug/TexasSegments.wkt"
    val data = spark.read.option("header", "false").option("delimiter", "\t")
      .csv(input).rdd.map{ row =>
        val wkt = row.getString(0)
        val lab = row.getString(1)

        (wkt, lab)
      }

    val results = data.map{ case(s,l) => (l,List(s)) }
      .reduceByKey{ case(a, b) => a ++ b }
      .mapPartitions{ it =>
        val reader = new WKTReader(geofactory)
        it.flatMap{ case(l,ss) =>
          val lines = ss.map(s => reader.read(s).asInstanceOf[LineString])
          val ps = mergeLines(lines)
          ps.map{ p => (l,p) }
        }
      }

    println(results.count)
    //results.take(100).foreach{println}


    spark.close
  }
}
