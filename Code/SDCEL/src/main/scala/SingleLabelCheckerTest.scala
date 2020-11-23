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
import SingleLabelChecker._

object SingleLabelCheckerTest{
  case class FaceT(face: Polygon, id: String, pid: Int = -1)

  def parseId(id: String): String = id.split("\\|").distinct.sorted.mkString("|")

  def checkSingleLabels(faces: RDD[FaceT])
      (implicit logger: Logger): RDD[(FaceT, FaceT)] = {
    faces.mapPartitionsWithIndex{ case(index, iter) =>
      val facesList = iter.toList
      val merged  = facesList.filter(_.id.contains("|"))
      val singles = facesList.filterNot(_.id.contains("|"))
      val singleA = singles.filter(_.id.substring(0,1) == "A")
      val singleB = singles.filter(_.id.substring(0,1) == "B")

      val facesA = for{
        A <- singleA
        B <- singleB union merged if {
          A.face.coveredBy(B.face)
        }
      } yield {
        (A, B)
      }
      
      val facesB = for{
        B <- singleB
        A <- singleA union merged if {
          B.face.coveredBy(A.face)
        }
      } yield {
        (B, A)
      }

      (facesA union facesB).toIterator
    }
  }

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
    val input = "tmp/faces"
    val parts = (s"hdfs dfs -ls ${input}/part*" #| "tail -n 1" !!)
      .split("/").reverse.head.split("-")(1).toInt
    val partitions = parts + 1
    val facesRDD = spark.read.text(input).rdd.map{ row =>
      val arr = row.getString(0).split("\t")
      val reader = new WKTReader(geofactory)
      val face = reader.read(arr(0)).asInstanceOf[Polygon]
      val id = arr(1)
      val pid = arr(2).toInt

      (pid, FaceT(face, id, pid))
    }.partitionBy(new SimplePartitioner(partitions))
      .map(_._2)
      .filter(_.face.isValid)
      .cache()
    logger.info("Reading data... Done!")
    val nPartitions = facesRDD.getNumPartitions
    logger.info(s"Number of partitions: $nPartitions")
    val nFacesRDD = facesRDD.count()
    logger.info(s"Number of records: $nFacesRDD")

    val toCheck = checkSingleLabels(facesRDD).cache()
    val nToCheck = toCheck.count

    logger.info(s"Records to check: $nToCheck")

    spark.close()
  }
}

class SimplePartitioner[V](partitions: Int) extends org.apache.spark.Partitioner {
  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[Int]
    // `k` is assumed to go continuously from 0 to elements-1.
    return k
  }

  def numPartitions(): Int = {
    return partitions
  }
}
