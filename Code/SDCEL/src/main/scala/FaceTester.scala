import scala.collection.JavaConverters._
import sys.process._
import com.vividsolutions.jts.geom.{Coordinate, Envelope, Geometry}
import com.vividsolutions.jts.geom.{LineString, LinearRing, Point, Polygon}
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{Row, Dataset, SparkSession}
import org.apache.spark.sql.functions
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator
import org.datasyslab.geosparkviz.sql.utils.GeoSparkVizRegistrator

import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.spatialPartitioning.quadtree._
import org.slf4j.{Logger, LoggerFactory}

object FaceTester{
  case class FacePartition(pid: Int, id: String, face: Vector[Polygon])

  def main(args: Array[String]): Unit = {
    implicit val logger: Logger = LoggerFactory.getLogger("myLogger")
    val model = new PrecisionModel(1000)
    val geofactory = new GeometryFactory(model)

    // Starting session...
    logger.info("Starting session...")
    val appName = "SingleLabelCheckerTester"
    implicit val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkVizKryoRegistrator].getName)
      .appName(appName)
      .getOrCreate()
    GeoSparkSQLRegistrator.registerAll(spark)
    GeoSparkVizRegistrator.registerAll(spark)
    import spark.implicits._
    logger.info("Starting session... Done!")

    logger.info("Reading data...")
    val input = "tmp/facesA"
    val parts = (s"hdfs dfs -ls ${input}/part*" #| "tail -n 1" !!)
      .split("/").reverse.head.split("-")(1).toInt
    val partitions = parts + 1
    val facesRDD = spark.read.text(input).rdd
      .filter(_.getString(0) != "")
      .map{ row =>
        val line = row.getString(0)
        
        val arr = line.split("\t")
        val pid = arr(0).toInt
        val id = arr(1)
        val n = arr(2).toInt
        val reader = new WKTReader(geofactory)
        val facePolygons = arr(3).split("\\|").map{ wkt =>
          try{
            reader.read(wkt).asInstanceOf[Polygon]
          } catch {
            case e: com.vividsolutions.jts.io.ParseException =>{
              println(s"$wkt\t$pid\t$id")
              geofactory.createPolygon(Array.empty[Coordinate])
            }
          }
        }.toVector

        (pid, FacePartition(pid, id, facePolygons))
      }.partitionBy(new SimplePartitioner(partitions))
      .map(_._2)
      .cache()
    logger.info("Reading data... Done!")
    val nPartitions = facesRDD.getNumPartitions
    logger.info(s"Number of partitions: $nPartitions")
    val nFacesRDD = facesRDD.count()
    logger.info(s"Number of records: $nFacesRDD")

    val f = new java.io.PrintWriter("/tmp/edgesALabels.wkt")
    val wkt = facesRDD.map{ face =>
      val n = face.id.split("\\|").length
      (n, face)
    }.filter(_._1 > 1).flatMap{ case(n, face) =>
        face.face.map{ f =>
          val wkt = f.toText
          s"$wkt\t${face.pid}\t${face.id}\n"
        }
    }.collect.mkString("")
    f.write(wkt)
    f.close
    
    spark.close()
  }
}

