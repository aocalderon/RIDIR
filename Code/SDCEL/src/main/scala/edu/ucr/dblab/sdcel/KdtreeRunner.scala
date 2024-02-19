package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, PrecisionModel}
import edu.ucr.dblab.sdcel.Utils.{Settings, debug, log, save}
import edu.ucr.dblab.sdcel.kdtree.KDBTree
import edu.ucr.dblab.sdcel.reader.PR_Utils.{getStudyArea, read}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.util.Random

object KdtreeRunner {
  def main(args: Array[String]): Unit = {
    // Starting session...
    implicit val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
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

    val n = edgesRDD.count.toInt
    val tree: KDBTree = new KDBTree(params.maxentries(), params.maxlevel(), study_area)
    val sample = edgesRDD.flatMap{_.getCoordinates.map( c => (Random.nextInt(n), G.createPoint(c)) )}.sortBy(_._1).map(_._2).collect()
    for(p <- sample) {
      tree.insert(p.getEnvelopeInternal)
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


    val tree2: KDBTree = new KDBTree(16, 16, study_area)
    val x0 = study_area.getMinX
    val y0 = study_area.getMinY
    val w = study_area.getWidth
    val h = study_area.getHeight

    val points = (0 until 1000).map{ i =>
      val x = x0 + Random.nextDouble() * w
      val y = y0 + Random.nextDouble() * h
      val p = G.createPoint(new Coordinate(x, y))
      p.setUserData(i)
      p
    }
    save("/tmp/edgesP.wkt"){
      points.map{_.toText + "\n"}
    }

    for(point <- points) {
      //println(point.getUserData.toString)
      tree2.insert(point.getEnvelopeInternal)
    }
    tree2.assignLeafIds()

    debug {
      save("/tmp/edgesCells2.wkt") {
        tree2.findLeafNodes(study_area).asScala.map { node =>
          val wkt = G.toGeometry(node.getExtent)
          val  id = node.getLeafId
          s"$wkt\t$id\n"
        }.toList
      }
    }



    spark.close
  }


}
