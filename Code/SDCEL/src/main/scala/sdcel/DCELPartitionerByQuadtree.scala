package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geom.{GeometryFactory, LineString, PrecisionModel}
import edu.ucr.dblab.sdcel.DCELPartitioner2.{read, read2, saveToHDFSWithCrossingInfo}
import edu.ucr.dblab.sdcel.PartitionReader.readQuadtree
import edu.ucr.dblab.sdcel.Utils._
import edu.ucr.dblab.sdcel.quadtree._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.slf4j.{Logger, LoggerFactory}

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
    log("TIMEP|Start")

    // Reading data...
    val edgesRDDA = if(!params.readid()) read(params.input1()) else read2(params.input1())
    val nEdgesRDDA = edgesRDDA.getRawSpatialRDD.count()
    log(s"INFO|edgesA=$nEdgesRDDA")
    val edgesRDDB = if(!params.readid()) read(params.input2()) else read2(params.input2())
    val nEdgesRDDB = edgesRDDB.getRawSpatialRDD.count()
    log(s"INFO|edgesB=$nEdgesRDDB")
    val edgesRDD = edgesRDDA.getRawSpatialRDD.rdd  union edgesRDDB.getRawSpatialRDD.rdd
    val nEdgesRDD = nEdgesRDDA + nEdgesRDDB
    log("TIMEP|Read")

    // Reading quatree...
    val (quadtree, cells) = readQuadtree[LineString](params.quadtree(), params.boundary())
    val partitioner = new QuadTreePartitioner(quadtree)
    edgesRDDA.spatialPartitioning(partitioner)
    val edgesA = edgesRDDA.spatialPartitionedRDD.rdd
    edgesRDDB.spatialPartitioning(partitioner)
    val edgesB = edgesRDDB.spatialPartitionedRDD.rdd

    // Saving to HDFS adding info about edges crossing border cells...
    saveToHDFSWithCrossingInfo(edgesA, cells, params.apath())
    saveToHDFSWithCrossingInfo(edgesB, cells, params.bpath())
    log("TIMEP|Saving")

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
