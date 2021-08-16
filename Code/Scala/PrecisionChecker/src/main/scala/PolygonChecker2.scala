import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory, Polygon}
import com.vividsolutions.jts.io.WKTReader
import com.vividsolutions.jts.index.strtree.STRtree

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{Row, Dataset, SparkSession}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.spatialOperator.JoinQuery

import collection.JavaConverters._
import scala.io.Source
import java.io.PrintWriter

object PolygonChecker2 {
  def main(args:Array[String]) = {
    val scale = 1000.0
    implicit val model = new PrecisionModel(scale)
    implicit val geofactory = new GeometryFactory(model)
    implicit val reader = new WKTReader(geofactory)


    implicit val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()
    import spark.implicits._

    //val inputA = "/home/and/Datasets/RIDIR/PH/PH_faces_parallel.wkt"
    val inputA = "/home/and/Datasets/RIDIR/TX/TX_faces_parallel.wkt"
    val polysARaw = read(inputA)
    val polysARDD = new SpatialRDD[Polygon]()
    polysARDD.setRawSpatialRDD(polysARaw)
    polysARDD.analyze()
    polysARDD.spatialPartitioning(GridType.QUADTREE, 40)
    val polysA = polysARDD.spatialPartitionedRDD.rdd.cache
    println("A read")
  
    //val inputB = "/home/and/Datasets/RIDIR/PH/PH_faces_sequential.wkt"
    val inputB = "/home/and/Datasets/RIDIR/TX/faces.wkt"
    val polysBRaw = read(inputB)
    val polysBRDD = new SpatialRDD[Polygon]()
    polysBRDD.setRawSpatialRDD(polysBRaw)
    polysBRDD.analyze()
    polysBRDD.spatialPartitioning(polysARDD.getPartitioner)
    val polysB = polysBRDD.spatialPartitionedRDD.rdd.cache
    println("B read")

    val considerBoundaryIntersection = false // Only return gemeotries fully covered by each query window in queryWindowRDD
    val buildOnSpatialPartitionedRDD = true // Set to TRUE only if run join query
    val usingIndex = true
    polysARDD.buildIndex(IndexType.QUADTREE, buildOnSpatialPartitionedRDD)
    val result = JoinQuery.SpatialJoinQueryFlat(polysARDD, polysBRDD, usingIndex, considerBoundaryIntersection)

    val data = result.rdd.map{ case(a,b) =>
      a.normalize()
      b.normalize()
      val ee1 = a.equalsExact(b, 0.1)
      val labA = a.getUserData.toString
      val labB = b.getUserData.toString

      //if(ee1) print(".")

      (labA, labB, ee1)
    }.filter{ case(a,b,r1) => r1 }
    println("Join done")

    write("/tmp/results.txt",
      data.mapPartitionsWithIndex{ (pid, it) =>
        it.map{ case(a,b,r1) =>
          s"$a\t$b\t$r1\n"
        }
      }.collect
    )
    println("Save done")

    spark.close
    
  }

  def read(filename: String)(implicit spark: SparkSession, geofactory: GeometryFactory)
      : RDD[Polygon] = {
    val polys = spark.read.option("delimiter","\t").option("header","false")
      .csv(filename).rdd.mapPartitions{ it =>
        val reader = new WKTReader(geofactory)
        it.map{ row =>
          val wkt = row.getString(0)
          val lab = row.getString(1)
          val poly = reader.read(wkt).asInstanceOf[Polygon]
          poly.setUserData(lab)
          poly
        }
    }
    polys
  }

  def read(filename: String, tree: STRtree)
    (implicit spark: SparkSession, geofactory: GeometryFactory)
      : RDD[Polygon] = {
    val polys = spark.read.option("delimiter","\t").option("header","false")
      .csv(filename).rdd.mapPartitions{ it =>
        val reader = new WKTReader(geofactory)
        it.map{ row =>
          val wkt = row.getString(0)
          val lab = row.getString(1)
          val poly = reader.read(wkt).asInstanceOf[Polygon]
          poly.setUserData(lab)
          poly
        }
    }
    polys
  }

  def write(filename: String, content: Array[String]): Unit = {
    val f1 = new PrintWriter(filename)
    f1.write(content.mkString(""))
    f1.close
  }
}
