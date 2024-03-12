package edu.ucr.dblab.sdcel.extension

import com.vividsolutions.jts.geom.{GeometryFactory, LineString, PrecisionModel}
import edu.ucr.dblab.sdcel.{DCELPartitioner2, Params, SimplePartitioner}
import edu.ucr.dblab.sdcel.Utils.{Settings, log, save}
import edu.ucr.dblab.sdcel.geometries.{Cell, EdgeData}
import edu.ucr.dblab.sdcel.reader.PR_Utils.read
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters.asScalaBufferConverter

object ScaleUpPartitioner {
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

    val edgesA = edgesRDDA.mapPartitions { edges =>
      edges.flatMap { edge =>
        tree.query(edge.getEnvelopeInternal).asScala.map { id =>
          (id.asInstanceOf[Int], edge)
        }
      }
    }.partitionBy(new SimplePartitioner[LineString](grids.size)).map(_._2)

    val edgesB = edgesRDDB.mapPartitions { edges =>
      edges.flatMap { edge =>
        tree.query(edge.getEnvelopeInternal).asScala.map { id =>
          (id.asInstanceOf[Int], edge)
        }
      }
    }.partitionBy(new SimplePartitioner[LineString](grids.size)).map(_._2)

    val kcells = grids.map{ case(id, poly) =>
      id -> Cell(id, "", SU_Utils.envelope2ring(poly.getEnvelopeInternal))
    }
    val A = DCELPartitioner2.getEdgesWithCrossingInfo(edgesA, kcells, "A").cache()
    val B = DCELPartitioner2.getEdgesWithCrossingInfo(edgesB, kcells, "B").cache()
    val na = A.count()
    val nb = B.count()
    log(s"INFO|$tag|Kdtree|nEdgesA|$na")
    log(s"INFO|$tag|Kdtree|nEdgesB|$nb")

    save("/tmp/edgesEA.wkt"){
      A.mapPartitionsWithIndex { (i, hs) =>
        hs.map { h =>
          val data = h.getUserData.asInstanceOf[EdgeData]
          s"${h.toText}\t$i\t${data.polygonId}\t${data.ringId}\t${data.edgeId}\t${data.isHole}\t${data.nedges}\t${data.crossingInfo}\n"
        }
      }.collect()
    }

    save("/tmp/edgesEB.wkt"){
      B.mapPartitionsWithIndex { (i, hs) =>
        hs.map { h =>
          val data = h.getUserData.asInstanceOf[EdgeData]
          s"${h.toText}\t$i\t${data.polygonId}\t${data.ringId}\t${data.edgeId}\t${data.isHole}\t${data.nedges}\t${data.crossingInfo}\n"
        }
      }.collect()
    }

    spark.close
  }
}
