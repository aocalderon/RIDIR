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

object SDCEL {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]) = {
    // Starting session...
    logger.info("Starting session...")
    implicit val params = new Params(args)
    val model = new PrecisionModel(params.scale())
    implicit val geofactory = new GeometryFactory(model)

    val (quadtree, cells) = readQuadtree[Int](params.quadtree(), params.boundary())
    save{"/tmp/edgesCells.wkt"}{
      cells.values.map{ cell =>
        val wkt = envelope2polygon(cell.mbr.getEnvelopeInternal).toText
        val id = cell.id
        val lineage = cell.lineage
        s"$wkt\t$id\t$lineage\n"
      }.toList
    }

    logger.info(s"Number of partitions: ${quadtree.getLeafZones.size()}")
    logger.info(s"Number of partitions: ${cells.size}")

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

    // Reading data...
    val edgesRDDA = readEdges(params.input1(), quadtree, "A")
    //edgesRDDA.persist()
    //val nEdgesRDDA = edgesRDDA.count()
    //println("Edges A: " + nEdgesRDDA)

    val edgesRDDB = readEdges(params.input2(), quadtree, "B")
    //edgesRDDB.persist()
    //val nEdgesRDDB = edgesRDDB.count()
    //println("Edges B: " + nEdgesRDDB)

    log("Reading data... Done!")

    // Getting LDCELs...
    val dcelsA = getLDCELs(edgesRDDA, cells)
    //dcelsA.persist()
    //val nA = dcelsA.count()
    log("Getting LDCELs for A... done!")

    val dcelsB = getLDCELs(edgesRDDB, cells)
    //dcelsB.persist()
    //val nB = dcelsB.count()
    log("Getting LDCELs for B... done!")

    
    if(params.debug()){
      save{"/tmp/edgesHA.wkt"}{
        dcelsA.mapPartitionsWithIndex{ (index, dcelsIt) =>
          val dcel = dcelsIt.next
          dcel.map{ h =>
            val wkt = h.getPolygon.toText
            val pid = h.data.polygonId
            val rid = h.data.ringId
            val eid = h.data.edgeId
            
            s"$wkt\t$pid:$rid:$eid\t$index\n"
          }.toIterator
        }.collect
      }

      save{"/tmp/edgesHB.wkt"}{
        dcelsB.mapPartitionsWithIndex{ (index, dcelsIt) =>
          val dcel = dcelsIt.next
          dcel.map{ h =>
            val wkt = h.getPolygon.toText
            val pid = h.data.polygonId
            val rid = h.data.ringId
            val eid = h.data.edgeId
            
            s"$wkt\t$pid:$rid:$eid\t$index\n"
          }.toIterator
        }.collect
      }
       
    }
    
    // Merging DCELs...
    val sdcel = dcelsA.zipPartitions(dcelsB, preservesPartitioning=true){ (iterA, iterB) =>
      val A = iterA.next.map(_.getNexts).flatten.toList
      val B = iterB.next.map(_.getNexts).flatten.toList

      val hedges = merge2(A, B)
      log("End of local merge...")

      hedges.toIterator
    }
    val nSDcel = sdcel.count()
    log("Merging DCELs... done!")

    if(params.debug()){
      save("/tmp/edgesH.wkt"){
        sdcel.map{ case(h, tag) =>
          val wkt = h.getPolygon.toText

          s"$wkt\t$tag\n"
        }.collect
      }
    }

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

  def log(msg: String)(implicit logger: Logger, settings: Settings): Unit = {
    logger.info(s"${settings.appId}|$msg")
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
