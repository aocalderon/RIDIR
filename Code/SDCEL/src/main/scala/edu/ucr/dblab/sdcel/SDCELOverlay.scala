package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geom._
import edu.ucr.dblab.sdcel.LocalDCEL.createLocalDCELs
import edu.ucr.dblab.sdcel.Utils.{Settings, log}
import edu.ucr.dblab.sdcel.cells.EmptyCellManager2.{getNonEmptyCells, runEmptyCells}
import edu.ucr.dblab.sdcel.geometries.Cell
import edu.ucr.dblab.sdcel.quadtree.{QuadRectangle, StandardQuadTree}
import edu.ucr.dblab.sdcel.reader.PR_Utils._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator

object SDCELOverlay {

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
      appId = spark.sparkContext.applicationId,
      output = params.output()
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

    val study_area = new QuadRectangle( getStudyArea(edgesRDD) ) // getStudyArea returns an Envelope...
    implicit val quadtree: StandardQuadTree[LineString] = new StandardQuadTree[LineString](study_area, 0, params.maxentries(), 10) // study area, level, max items, max level...
    val sample = edgesRDD.sample(withReplacement = false, params.fraction(), 42).collect() // replacement, sample size, random seed...+
    sample.foreach{ edge =>
      quadtree.insert(new QuadRectangle(edge.getEnvelopeInternal), edge)
    }
    quadtree.assignPartitionIds()
    quadtree.assignPartitionLineage()
    implicit val cells: Map[Int, Cell] = getCells(quadtree)

    val edgesA = partitionEdgesByQuadtree(edgesRDDA, quadtree, "A")
    val edgesB = partitionEdgesByQuadtree(edgesRDDB, quadtree, "B")

    val non_emptiesA = getNonEmptyCells(edgesA)
    val non_emptiesB = getNonEmptyCells(edgesB, "B")

    val ldcelA = createLocalDCELs(edgesA)
    val ma = runEmptyCells(ldcelA, non_emptiesA)

    val ldcelB = createLocalDCELs(edgesB, "B")
    val mb = runEmptyCells(ldcelB, non_emptiesB, "B")

    DCELOverlay2.overlay(ldcelA, ma, ldcelB, mb)

    spark.close
  }
}
