package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geom.{Geometry, Polygon, LinearRing}
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.slf4j.{Logger, LoggerFactory}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.enums.GridType

import collection.JavaConverters._

import PartitionReader._
import edu.ucr.dblab.sdcel.DCELBuilder2.save
import edu.ucr.dblab.sdcel.PolygonChecker2._
import edu.ucr.dblab.sdcel.geometries.Cell
import edu.ucr.dblab.sdcel.quadtree.Quadtree

object PolygonExtractor {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")
 
  val SCALE = 10e13

  def main(args: Array[String]): Unit = {
    logger.info("Starting session.")
    implicit val params = new Params(args)
    val model = new PrecisionModel(SCALE)
    implicit val geofactory = new GeometryFactory(model)
    
    val (quadtree, cells) = readQuadtree[Int](params.quadtree(), params.boundary())

    implicit val spark = SparkSession.builder()
        .config("spark.serializer",classOf[KryoSerializer].getName)
        .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
        .getOrCreate()
    import spark.implicits._

    logger.info("Reading data...")
    val filter = "2122110"
    //val hdfs_path = params.input1()

    val q = Quadtree.get(quadtree, filter)
    save(s"${params.output()}/boundary.wkt"){List(q.getZone.wkt())}
    save(s"${params.output()}/quadtree.wkt"){q.getLeafZones.asScala.map(_.lineage)}

    val ids = getCellIds(cells, filter)
      .map( i => i.toString.reverse.padTo(5, '0').reverse ).toSet

    def getMergerCmd(hdfs_path: String, tag: String): String = {
      import sys.process._
      val cmd = Seq("hdfs","dfs","-ls",hdfs_path)
      val res = cmd.!!

      val total = res.split("\n").filter(_.contains(hdfs_path)).map(_.split(" ").last).toSet

      val pattern = total.last.split("/").last.split("-").slice(2,20).mkString("-")
      val sample = ids.map(i => s"${hdfs_path}/part-${i}-${pattern}")

      val files = total.intersect(sample)

      val merge = Seq("hdfs", "dfs", "-getmerge", files.mkString(" "),
        s"${params.output()}/${tag}.txt")

      merge.mkString(" ")
    }
    
    val mergeA = getMergerCmd(params.input1(), "A")
    println
    println(mergeA)
    val mergeB = getMergerCmd(params.input2(), "B")
    println
    println(mergeB)

    spark.close
  }

  def getCellIds(cells: Map[Int,Cell], filter: String): Iterable[Int] = {
    cells.filter{ case(id, cell) =>
      if(cell.lineage.size >= filter.size){
        cell.lineage.substring(0, filter.size) == filter
      } else {
        false
      }
    }.keys    
  }
}
