package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geom.{Coordinate, Envelope, GeometryFactory, PrecisionModel}
import edu.ucr.dblab.sdcel.Utils.{Settings, debug, log, save}
import edu.ucr.dblab.sdcel.kdtree.KDBTree
import edu.ucr.dblab.sdcel.reader.PR_Utils.{getStudyArea, read}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator

import scala.collection.JavaConverters.{asScalaBufferConverter, mapAsScalaMapConverter}
import scala.util.Random

object KdtreeRunner {
  def main(args: Array[String]): Unit = {
    // Starting session...
    implicit val spark: SparkSession = SparkSession.builder()
      .master("local[3]")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()
    val params = new Params(args)
    implicit val S: Settings = Settings(
      tolerance = params.tolerance(),
      debug = params.debug(),
      appId = spark.sparkContext.applicationId
    )
    val command = System.getProperty("sun.java.command")
    log(s"COMMAND|$command")
    log(s"INFO|scale=${S.scale}")
    val model = new PrecisionModel(S.scale)
    implicit val G: GeometryFactory = new GeometryFactory(model)
    log("TIME|Start")

/*
    // Reading data...
    val edgesRDDA = read(params.input1())
    val nEdgesRDDA = edgesRDDA.count()
    log(s"INFO|edgesA=$nEdgesRDDA")
    val edgesRDDB = read(params.input2())
    val nEdgesRDDB = edgesRDDB.count()
    log(s"INFO|edgesB=$nEdgesRDDB")
    val edgesRDD = edgesRDDA.union( edgesRDDB ).cache()
    val nEdgesRDD = nEdgesRDDA + nEdgesRDDB
    log(s"INFO|TotalEdges=$nEdgesRDD")
    log("TIME|Read")

    val study_area = getStudyArea(edgesRDD) // getStudyArea returns an Envelope...
    edgesRDD.count()

    debug {
      save("/tmp/edgesE.wkt") {
        edgesRDD.map { edge =>
          val info = edge.getUserData.asInstanceOf[String]
          val wkt = edge.toText

          s"$wkt\t$info\n"
        }.collect
      }
    }

    val tree: KDBTree = new KDBTree(params.maxentries(), params.maxlevel(), study_area)
    val sample = edgesRDD.sample(withReplacement = false, params.fraction(), 42).map(edge => {
      val i = Random.nextLong()
      (i, edge)
    }).sortBy(_._1).map(_._2).collect
    for(edge <- sample) {
      tree.insert(edge.getEnvelopeInternal)
    }
    tree.assignLeafIds()

    debug {
      save("/tmp/edgesCells.wkt") {
        tree.findLeafNodes(study_area).asScala.map { node =>
          val wkt = G.toGeometry(node.getExtent)
          val  id = node.getLeafId
          s"$wkt\t$id\n"
        }.toList
      }
    }

 */

    val W = 10
    val H = 10
    val n = 20
    val points = for(i <- 0 until n) yield {
      val x = Random.nextInt(W)
      val y = Random.nextInt(H)
      val p = G.createPoint(new Coordinate(x, y))
      p.setUserData(i)
      p
    }
    save("/tmp/edgesS.wkt"){
      points.map{ point =>
        val wkt = point.toText
        val  id = point.getUserData.asInstanceOf[Int]

        s"$wkt\t$id\n"
      }
    }

    val envelope = new Envelope(0, W, 0, H)
    val sorted_tree: KDBTree = new KDBTree(5, 8, envelope)
    points.foreach{ point =>
      sorted_tree.insert(point.getEnvelopeInternal)
    }
    sorted_tree.assignLeafIds()

    save("/tmp/edgesCells.wkt") {
      sorted_tree.getLeaves.asScala.map { case (id, envelope) =>
        val wkt = G.toGeometry(envelope).toText
        s"$wkt\t$id\n"
      }.toList
    }

    save("/tmp/intervals.tsv") {
      sorted_tree.getIntervals.asScala.map { case (id, intervals) =>
        val is = intervals.asScala.mkString(", ")
        s"$id\t$is\n"
      }.toList
    }

    spark.close
  }


}
