package edu.ucr.dblab.sdcel

import org.slf4j.{LoggerFactory, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.rdd.RDD
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.spatialOperator.RangeQuery
import org.datasyslab.geospark.utils.GeoSparkConf
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import com.vividsolutions.jts.io.WKTReader
import com.vividsolutions.jts.geom.{Geometry, Polygon, Coordinate, Envelope, GeometryFactory, PrecisionModel}
import org.rogach.scallop._

import scala.io.Source
import scala.collection.JavaConverters._

import edu.ucr.dblab.sdcel.quadtree.{Quadtree, StandardQuadTree}
import edu.ucr.dblab.sdcel.geometries.Cell
import edu.ucr.dblab.sdcel.PartitionReader.{readQuadtree, readEdges, envelope2polygon}
import edu.ucr.dblab.sdcel.Utils._

object CellAgg{
  case class C(id: Int, lineage: String)

  def main(args: Array[String]): Unit = {
    implicit val logger: Logger = LoggerFactory.getLogger("myLogger")
    logger.info("Starting session...")
    val params = new CellAggConf(args)
    val delimiter = params.delimiter()
    implicit val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .appName("Quadtree_Cell_Aggregator")
      .getOrCreate()
    val appId = spark.sparkContext.getConf.get("spark.app.id")
    val command = System.getProperty("sun.java.command")
    implicit val settings = Settings(
      tolerance = params.tolerance(),
      appId = appId
    )
    
    logger.info(s"${appId}|${command}")
    implicit val model = new PrecisionModel(1/params.tolerance())
    implicit val geofactory = new GeometryFactory(model)
    import spark.implicits._

    logger.info("Reading data...")
    val home = "/home/acald013/RIDIR/local_path"
    val qpath = s"$home/${params.input()}/quadtree.wkt"
    val bpath = s"$home/${params.input()}/boundary.wkt"
    val (quadtree, cells) = readQuadtree[Int](qpath, bpath)
    val dpath = s"${params.input()}/edgesA"
    val edges = readEdges(dpath, quadtree, "A").cache

    logger.info("Processing data...")
    val level = params.level()
    val lids1 = cells.values.map{ cell =>
      val cid = cell.id
      val lin = cell.lineage 
      val lid = if( lin.size < level ) "" else lin.substring(0, level)
      (lid, cid)
    }.toList
    val lids2 = lids1.map(_._1).distinct.sorted.zipWithIndex
    val new_partitions = lids2.size
    val cids = for{ a <- lids1; b <- lids2; if a._1 == b._1 } yield { a._2 -> b._2 }
    val oldcids2newcids = cids.toMap
    spark.sparkContext.broadcast(oldcids2newcids)
    val edges2 = edges.mapPartitionsWithIndex{ (oldcid, it) =>
      val newcid = oldcids2newcids(oldcid)
      it.map{ edge =>
        (newcid, edge)
      }
    }.partitionBy(new SimplePartitioner(new_partitions))
    .map(_._2)

    logger.info("Saving data...")
    save(params.output()){
      edges2.mapPartitionsWithIndex{ (cid, it) =>
        it.map{ edge =>
          val wkt = edge.toText
          s"$wkt\t$cid\n"
        }
      }.collect
    }
    save(params.doutput()){
      edges.mapPartitionsWithIndex{ (pid, it) =>
        it.map{ edge =>
          val wkt = edge.toText
          s"$wkt\t$pid\n"
        }
      }.collect
    }
    save(params.q1output()){
      cells.values.map{ c =>
        val wkt = c.wkt
        val cid = c.id
        val lin = c.lineage
        s"$wkt\t$cid\t$lin\n"
      }.toList
    }
    save(params.q2output()){
      edges2.mapPartitionsWithIndex{ (pid, it) =>
        val wkt = oldcids2newcids.filter(_._2 == pid).map{ c =>
          val cid = c._1
          val cell = cells(cid)
          cell.toPolygon
        }.reduce{ (a, b) =>
          val envelope = a.getEnvelopeInternal
          envelope.expandToInclude(b.getEnvelopeInternal)
          envelope2polygon(envelope)
        }.toText
        Iterator(s"$wkt\t$pid\n")
      }.collect
    }

    logger.info("Clossing session...")
    spark.close()
  }
  
}

class CellAggConf(args: Seq[String]) extends ScallopConf(args) {
  val input: ScallopOption[String]     = opt[String]  (required = true)
  val level: ScallopOption[Int]        = opt[Int]     (required = true)
  val output: ScallopOption[String]    = opt[String]  (default = Some("/tmp/edgesO.wkt"))
  val doutput: ScallopOption[String]   = opt[String]  (default = Some("/tmp/edgesD.wkt"))
  val q1output: ScallopOption[String]  = opt[String]  (default = Some("/tmp/edgesQ1.wkt"))
  val q2output: ScallopOption[String]  = opt[String]  (default = Some("/tmp/edgesQ2.wkt"))
  val delimiter: ScallopOption[String] = opt[String]  (default = Some("\t"))
  val tolerance: ScallopOption[Double] = opt[Double]  (default = Some(1e-3))


  verify()
}
