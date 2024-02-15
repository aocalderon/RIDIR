package edu.ucr.dblab.sdcel.reader

import com.vividsolutions.jts.geom._
import edu.ucr.dblab.sdcel.Params
import edu.ucr.dblab.sdcel.Utils.{Settings, log, save}
import edu.ucr.dblab.sdcel.quadtree.{QuadRectangle, StandardQuadTree}
import edu.ucr.dblab.sdcel.reader.PR_Utils._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator

object PolygonsReader {

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

    debug {
      save("/tmp/edgesE.wkt") {
        edgesRDD.map { edge =>
          val info = edge.getUserData.asInstanceOf[String]
          val wkt = edge.toText

          s"$wkt\t$info\n"
        }.collect
      }
    }

    val study_area = new QuadRectangle( getStudyArea(edgesRDD) ) // getStudyArea returns an Envelope...
    val quadtree = new StandardQuadTree[LineString](study_area, 0, params.maxentries(), 10) // study area, level, max items, max level...
    val sample = edgesRDD.sample(withReplacement = false, params.fraction(), 42).collect() // replacement, sample size, random seed...+
    val nSample = sample.length
    sample.foreach{ edge =>
      quadtree.insert(new QuadRectangle(edge.getEnvelopeInternal), edge)
    }
    val n = quadtree.getLeafZones.size()
    quadtree.assignPartitionIds()
    quadtree.assignPartitionLineage()
    val cells = getCells(quadtree)

    debug{
      save("/tmp/edgesCells.wkt"){
        cells.values.toList.map(_.wkt + "\n")
      }
    }

    spark.close
  }
}
