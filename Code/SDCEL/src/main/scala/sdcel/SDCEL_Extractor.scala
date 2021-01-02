package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geom.LineString
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.TaskContext
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.slf4j.{Logger, LoggerFactory}
import edu.ucr.dblab.sdcel.quadtree._
import edu.ucr.dblab.sdcel.geometries._
import PartitionReader._
import DCELBuilder2.getLDCELs
import DCELMerger2.merge2

object SDCEL_Extract {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]) = {
    // Starting session...
    logger.info("Starting session...")
    implicit val params = new Params(args)
    val model = new PrecisionModel(params.scale())
    implicit val geofactory = new GeometryFactory(model)

    val filter = params.filter()
    val (quadtree, cells) = filterQuadtree[Int](params.quadtree(), params.boundary(),
      filter)
    cells foreach println

    logger.info(s"Number of partitions: ${quadtree.getLeafZones.size()}")
    logger.info(s"Number of partitions: ${cells.size}")

    implicit val spark = SparkSession.builder()
        .config("spark.serializer",classOf[KryoSerializer].getName)
        .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
        .getOrCreate()
    import spark.implicits._
    logger.info("Starting session... Done!")

    val edgesRDDA = filterEdges(params.input1(), quadtree, "A")
    val edgesRDDB = filterEdges(params.input2(), quadtree, "B")

    val output = params.output()
    save(s"${output}/quadtree.wkt"){
      cells.values.map{ cell =>
        cell.lineage + "\n"
      }.toList
    }
    save(s"${output}/boundary.wkt"){
      val boundary = envelope2polygon(quadtree.getZone.getEnvelope)
      List(boundary.toText())
    }
    save(s"${output}/A.wkt"){
      edgesRDDA.mapPartitionsWithIndex{ (pid, edges) =>
        edges.map{ edge =>
          val wkt = edge.toText()
          val data = edge.getUserData.asInstanceOf[EdgeData]
          val polyId = data.polygonId
          val ringId = data.ringId
          val edgeId = data.edgeId
          val isHole = data.isHole

          s"$wkt\t$pid\t$polyId\t$ringId\t$edgeId\t$isHole\n"
        }
      }.collect
    }
    save(s"${output}/B.wkt"){
      edgesRDDB.mapPartitionsWithIndex{ (pid, edges) =>
        edges.map{ edge =>
          val wkt = edge.toText()
          val data = edge.getUserData.asInstanceOf[EdgeData]
          val polyId = data.polygonId
          val ringId = data.ringId
          val edgeId = data.edgeId
          val isHole = data.isHole

          s"$wkt\t$pid\t$polyId\t$ringId\t$edgeId\t$isHole\n"
        }
      }.collect
    }

    spark.close
  }  

  def save(filename: String)(content: Seq[String]): Unit = {
    val start = clocktime
    val f = new java.io.PrintWriter(filename)
    f.write(content.mkString(""))
    f.close
    val end = clocktime
    val time = "%.2f".format((end - start) / 1000.0)
    logger.info(s"Saved ${filename} in ${time}s [${content.size} records].")
  }
  private def clocktime = System.currentTimeMillis()
}
