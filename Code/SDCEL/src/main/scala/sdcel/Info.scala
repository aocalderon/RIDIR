package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geom.Polygon
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{SparkSession, Dataset, SaveMode}
import org.apache.spark.TaskContext
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.slf4j.{Logger, LoggerFactory}

import PartitionReader.{readQuadtree, readEdges, envelope2polygon}
import edu.ucr.dblab.sdcel.SDCEL.{log, save}
import edu.ucr.dblab.sdcel.geometries.Settings

object Info {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]) = {
    // Starting session...
    logger.info("Starting session...")
    implicit val params = new Params(args)
    val model = new PrecisionModel(params.scale())
    implicit val geofactory = new GeometryFactory(model)

    /*
    val (quadtree, cells) = readQuadtree[Int](params.quadtree(), params.boundary())
    save{"/tmp/edgesCells.wkt"}{
      cells.values.map{ cell =>
        val wkt = envelope2polygon(cell.mbr.getEnvelopeInternal).toText
        val id = cell.id
        val lineage = cell.lineage
        s"$wkt\t$id\t$lineage\n"
      }.toList
    }
    logger.info(s"Number of partitions: ${cells.size}")
     */

    implicit val spark = SparkSession.builder()
        .config("spark.serializer",classOf[KryoSerializer].getName)
        .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
        .getOrCreate()
    import spark.implicits._
    val conf = spark.sparkContext.getConf
    val appId = conf.get("spark.app.id")
    implicit val settings = Settings(appId)
    val command = System.getProperty("sun.java.command")
    log(command)
    log("Starting session... Done!")
    
    def getInfo(path: String): Dataset[(Int, Int)]= {
      spark.read.textFile(path).map{ line =>
        val arr = line.split("\t")
        arr(0)
      }.mapPartitions{ wkts =>
        val reader = new com.vividsolutions.jts.io.WKTReader(geofactory)
        val polys = wkts.map{ wkt => reader.read(wkt).asInstanceOf[Polygon] }.toList
        val edges = polys.map{_.getNumPoints}
        val npolys = polys.size
        val nedges = edges.foldLeft(0)(_ + _)

        Iterator((npolys, nedges))
      }
    }
    val A = getInfo("gadm/raw/level1").collect()
    println(s"#Polygons: ${A.map(_._1).reduce(_ + _)}")
    println(s"#Edges: ${A.map(_._2).reduce(_ + _)}")

    // Collecting number of edges per partition...
    /*
    val edgesRDDA = readEdges(params.input1(), quadtree, "A")
    val edgesRDDB = readEdges(params.input2(), quadtree, "B")
    save{"/tmp/edgesGrids.wkt"}{
      edgesRDDA.zipPartitions(edgesRDDB, preservesPartitioning=true){ (iterA, iterB) =>
        val i = TaskContext.getPartitionId
        val cell = cells(i)
        val wkt = cell.wkt
        val centroid = cell.toPolygon.getCentroid
        val x = centroid.getX
        val y = centroid.getY
        val n = iterA.size + iterB.size
        Iterator(s"$wkt\t$x\t$y\t$n\n")
      }.collect
    }
     */

    spark.close
  }
}
