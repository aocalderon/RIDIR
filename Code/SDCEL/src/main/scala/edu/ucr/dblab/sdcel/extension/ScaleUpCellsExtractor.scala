package edu.ucr.dblab.sdcel.extension

import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel}
import edu.ucr.dblab.sdcel.Params
import edu.ucr.dblab.sdcel.Utils.{Settings, log, save}
import edu.ucr.dblab.sdcel.reader.PR_Utils.read
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters.asScalaBufferConverter

object ScaleUpCellsExtractor {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]): Unit = {
    // Starting session...
    implicit val params: Params = new Params(args)
    implicit val spark: SparkSession = SparkSession.builder()
      .master(params.master())
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()
    implicit val S: Settings = Settings(
      tolerance = params.tolerance(),
      debug = params.debug(),
      appId = spark.sparkContext.applicationId
    )
    val tag = params.tag()
    val command = System.getProperty("sun.java.command")
    log(s"COMMAND|$command")

    log("TIME|Start")

    log(s"INFO|$tag|scale|${S.scale}")
    implicit val model: PrecisionModel = new PrecisionModel(S.scale)
    implicit val G: GeometryFactory = new GeometryFactory(model)

    val (grids, tree) = SU_Utils.gridPartitions(params.kpath())

    // Reading data...
    val edgesRDDA = read(params.input1())
    val nEdgesRDDA = edgesRDDA.count()
    log(s"INFO|$tag|edgesA|$nEdgesRDDA")
    val edgesRDDB = read(params.input2())
    val nEdgesRDDB = edgesRDDB.count()
    log(s"INFO|$tag|edgesB|$nEdgesRDDB")

    val Acells = edgesRDDA.mapPartitions{ edges =>
      edges.flatMap{ edge =>
        tree.query(edge.getEnvelopeInternal).asScala.map{ _.asInstanceOf[Int] }
      }
    }.distinct().collect()
    val Bcells = edgesRDDB.mapPartitions{ edges =>
      edges.flatMap{ edge =>
        tree.query(edge.getEnvelopeInternal).asScala.map{ _.asInstanceOf[Int] }
      }
    }.distinct().collect()
    val cells_ids = (Acells ++ Bcells).distinct
    val sample_grids = grids.filter{ case(id, _) => cells_ids.contains(id) }

    save(params.output()){
      sample_grids.toList.zipWithIndex.map{ case(mbr, index) =>
        val wkt = mbr._2.toText

        s"$wkt\t$index\n"
      }
    }

    spark.close
  }
}
