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

object SDCEL_Filter {
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

    logger.info(s"Number of partitions: ${quadtree.getLeafZones.size()}")
    logger.info(s"Number of partitions: ${cells.size}")

    implicit val spark = SparkSession.builder()
        .config("spark.serializer",classOf[KryoSerializer].getName)
        .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
        .getOrCreate()
    import spark.implicits._
    logger.info("Starting session... Done!")

    // Reading data...
    val edgesRDDA = filterEdges(params.input1(), quadtree, "A")
    val edgesRDDB = filterEdges(params.input2(), quadtree, "B")
    logger.info("Reading data... Done!")

    // Getting LDCELs...
    val dcelsA = getLDCELs(edgesRDDA, cells)
    logger.info("Getting LDCELs for A... done!")

    val dcelsB = getLDCELs(edgesRDDB, cells)
    logger.info("Getting LDCELs for B... done!")

    // Merging DCELs...
    val sdcel = dcelsA.zipPartitions(dcelsB, preservesPartitioning=true){ (iterA, iterB) =>
      val A = iterA.next.map(_.getNexts).flatten.toList
      val B = iterB.next.map(_.getNexts).flatten.toList

      
      val hedges = merge2(A, B, debug=false, max=(A.size + B.size))

      hedges.toIterator
    }.persist()
    val nSDcel = sdcel.count()
    logger.info("Merging DCELs... done!")

    sdcel.filter(_._2 == "Error").foreach(println)

    if(params.save()){
      sdcel.map{ case(h, tag) =>
          val wkt = h.getPolygon.toText
          s"$wkt"
      }.toDS.write
        .format("text").mode(SaveMode.Overwrite).save("gadm/output")
      logger.info("Saving results at gadm/output.")
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
