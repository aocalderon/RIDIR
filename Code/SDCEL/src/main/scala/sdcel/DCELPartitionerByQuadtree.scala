package edu.ucr.dblab.sdcel

import scala.collection.JavaConverters._
import com.vividsolutions.jts.geom.{Coordinate, Envelope}
import com.vividsolutions.jts.geom.{LineString, Polygon}
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.io.WKTReader
import com.vividsolutions.jts.algorithm.CGAlgorithms
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.slf4j.{Logger, LoggerFactory}

import edu.ucr.dblab.sdcel.quadtree._

import DCELPartitioner2.{read, saveToHDFSWithCrossingInfo}
import PartitionReader.readQuadtree
import Utils._

object DCELPartitionerByQuadtree {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")
  def main(args: Array[String]) = {
    // Starting session...
    implicit val spark = SparkSession.builder()
        .config("spark.serializer",classOf[KryoSerializer].getName)
        .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
        .getOrCreate()
    import spark.implicits._
    val params = new Params(args)
    implicit val settings = Settings(
      tolerance = params.tolerance(),
      debug = params.debug(),
      local = params.local(),
      appId = spark.sparkContext.applicationId
    )
    val command = System.getProperty("sun.java.command")
    log(s"COMMAND|$command")
    log(s"INFO|scale=${settings.scale}")
    val model = new PrecisionModel(settings.scale)
    implicit val geofactory = new GeometryFactory(model)
    log("TIME|Start")

    // Reading data...
    val edgesRDDA = read(params.input1())
    val nEdgesRDDA = edgesRDDA.getRawSpatialRDD.count()
    log(s"INFO|edgesA=$nEdgesRDDA")
    val edgesRDDB = read(params.input2())
    val nEdgesRDDB = edgesRDDB.getRawSpatialRDD.count()
    log(s"INFO|edgesB=$nEdgesRDDB")
    val edgesRDD = edgesRDDA.getRawSpatialRDD.rdd  union edgesRDDB.getRawSpatialRDD.rdd
    val nEdgesRDD = nEdgesRDDA + nEdgesRDDB
    log("TIME|Read")

    // Reading quatree...
    val (quadtree, cells) = readQuadtree[LineString](params.quadtree(), params.boundary())
    val partitioner = new QuadTreePartitioner(quadtree)
    edgesRDDA.spatialPartitioning(partitioner)
    val edgesA = edgesRDDA.spatialPartitionedRDD.rdd
    edgesRDDB.spatialPartitioning(partitioner)
    val edgesB = edgesRDDB.spatialPartitionedRDD.rdd

    // Saving to HDFS adding info about edges crossing border cells...
    if(params.save()){
      saveToHDFSWithCrossingInfo(edgesA, cells, params.apath())
      saveToHDFSWithCrossingInfo(edgesB, cells, params.bpath())
      log("TIME|Saving")
    }

    if(settings.debug){
      save("/tmp/edgesA.wkt"){
        edgesA.mapPartitionsWithIndex{ (pid, it) =>
          val cell = cells(pid)
          it.filter(_.intersects(cell.toPolygon)).map{ line =>
            val wkt = line.toText()
            s"$wkt\t$pid\n"
          }
        }.collect
      }
      save("/tmp/edgesB.wkt"){
        edgesB.mapPartitionsWithIndex{ (pid, it) =>
          val cell = cells(pid)
          it.filter(_.intersects(cell.toPolygon)).map{ line =>
            val wkt = line.toText()
            s"$wkt\t$pid\n"
          }
        }.collect
      }
      save("/tmp/edgesC.wkt"){
        cells.values.toList.map(_.wkt)
      }
    }

    spark.close
  }

}
