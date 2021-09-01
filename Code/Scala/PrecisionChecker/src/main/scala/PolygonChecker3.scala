import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory, Polygon}
import com.vividsolutions.jts.io.WKTReader
import com.vividsolutions.jts.algorithm.CGAlgorithms

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

object PolygonChecker3 {
  case class Record(label: String, wkt: String, tag: String)

  def main(args:Array[String]) = {
    val scale = 1000.0
    implicit val model = new PrecisionModel(scale)
    implicit val geofactory = new GeometryFactory(model)

    implicit val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()
    import spark.implicits._
    println("Start")

    val inputA = "file:///home/acald013/Datasets/CA/CGAL2/edgesFE.wkt"
    //val inputA = "/home/and/Datasets/RIDIR/TX/TX_faces_parallel.wkt"
    val polysA = read(inputA, "par")
    println("A read")
  
    val inputB = "file:///home/acald013/Datasets/CA/CGAL2/faces.wkt"
    //val inputB = "/home/and/Datasets/RIDIR/TX/faces.wkt"
    val polysB = read(inputB, "seq")
    println("B read")

    polysA//.filter($"label" === "A204 B62")
      .show
    polysB//.filter($"label" === "A204 B62")
      .show

    val joined0 = polysA.select($"label".as("label1"), $"wkt".as("wkt1"), $"tag".as("tag1"))
      .join(polysB, $"label1" === $"label" )
      .select($"label",$"wkt1",$"wkt".as("wkt2")).rdd
      .map{ row =>
        val lab = row.getString(0)
        val wkt1 = row.getString(1)
        val wkt2 = row.getString(2)
        (lab, wkt1, wkt2)
      }

    val joined = joined0.groupBy{ case(l,w1,w2) => (l,w1)}
      .mapPartitions{ it =>
        val reader = new WKTReader(geofactory)
        it.map{ case(k, l) =>
          val lab = k._1
          //val p1 = process(k._2, reader, lab)
          if(lab == "A204 B62"){
            println("wkt")
            println(k._2)
          }
          val p11 = reader.read(k._2).asInstanceOf[Polygon]
          if(lab == "A204 B62"){
            println("p11")
            println(p11)
          }
          val p1 = norm2(p11)
          if(lab == "A204 B62"){
            println("p1")
            println(p1)
            println
          }

          val ps = l.map{ w =>
            if(lab == "A204 B62"){
              println("wkt2")
              println(w._3)
            }
            val p22 = reader.read(w._3).asInstanceOf[Polygon]
            if(lab == "A204 B62"){
              println("p22")
              println(p22)
            }
            val p2 = norm2(p22)
            //val p2 = process(w._3, reader, lab)
            val b = p1.equalsExact(p2, 1)
            if(lab == "A204 B62"){
              println("p2")
              println(p2)
              println(b)
            }
            b
          }


          (lab, ps.exists(_ == true), p1.toText)
        }
      }.cache
      
    write("/tmp/edgesT.wkt",
      joined.filter(!_._2).map{ case(l,b,p) =>
        s"$p\t$l\n"
      }.collect
    )

    println(joined.count)
    println(joined.filter(_._2).count)
    println(joined.filter(!_._2).count)

    /*
    write("/tmp/results.txt",
      data.mapPartitionsWithIndex{ (pid, it) =>
        it.map{ case(a,b,r1) =>
          s"$a\t$b\t$r1\n"
        }
      }.collect
    )
    println("Save done")
     */

    spark.close
    
  }

  def process(wkt: String, reader: WKTReader, lab: String = "")
    (implicit geofactory: GeometryFactory): Polygon = {

    val p1_prime = reader.read(wkt).asInstanceOf[Polygon]
    //p1_prime.normalize
    val p1 = norm2(p1_prime)
    p1
  }

  def norm2(poly: Polygon)(implicit geofactory: GeometryFactory): Polygon = {
    val coords = poly.getCoordinates
    val minX = coords.minBy(_.x)
    val minXs = coords.filter(_.x == minX.x)
    val start = minXs.minBy(_.y)

    val coords2 = coords.slice(0, coords.size - 1)
    val i = coords2.indexOf(start)
    val (tail, head) = coords2.splitAt(i)

    val new_coords = head ++ tail :+ start

    geofactory.createPolygon(new_coords)
  }

  def read(filename: String, tag: String)
    (implicit spark: SparkSession, geofactory: GeometryFactory): Dataset[Record] = {
    import spark.implicits._
    val polys = spark.read.option("delimiter","\t").option("header","false")
      .csv(filename).mapPartitions{ it =>
        it.map{ row =>
          val wkt = row.getString(0)
          //val p = reader.read(wkt).asInstanceOf[Polygon]
          //val c1 = p.getCoordinates
          //val c2 = if(CGAlgorithms.isCCW(c1)) c1.reverse else c1
          //val p2 = geofactory.createPolygon(c1)
          val lab = row.getString(1).trim()
          Record(lab, wkt /*p2.toText*/, tag)
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
