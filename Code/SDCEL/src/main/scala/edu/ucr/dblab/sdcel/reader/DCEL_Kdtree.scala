package edu.ucr.dblab.sdcel.reader

import com.vividsolutions.jts.geom.{GeometryFactory, LineString, PrecisionModel}
import edu.ucr.dblab.sdcel.LocalDCEL.createLocalDCELs
import edu.ucr.dblab.sdcel.Utils.{Settings, log, save}
import edu.ucr.dblab.sdcel.cells.EmptyCellManager2.{EmptyCell, getNonEmptyCells}
import edu.ucr.dblab.sdcel.geometries.{Cell, EdgeData}
import edu.ucr.dblab.sdcel.kdtree.KDBTree
import edu.ucr.dblab.sdcel.quadtree.{QuadRectangle, StandardQuadTree}
import edu.ucr.dblab.sdcel.reader.PR_Utils._
import edu.ucr.dblab.sdcel.{DCELOverlay2, Params}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator

import scala.util.Random

object DCEL_Kdtree {
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
    val edgesRDD = edgesRDDA.union(edgesRDDB).cache()
    val nEdgesRDD = nEdgesRDDA + nEdgesRDDB
    log(s"INFO|TotalEdges=$nEdgesRDD")

    // Build quadtree...
    val envelope_area = getStudyArea(edgesRDD) // getStudyArea returns an Envelope...
    val rectangle = new QuadRectangle(envelope_area)
    implicit val quadtree: StandardQuadTree[LineString] = new StandardQuadTree[LineString](rectangle, 0, params.maxentries(), 10) // study area, level, max items, max level...
    val sample_quadtree = edgesRDD.sample(withReplacement = false, params.fraction(), 42).collect() // replacement, sample size, random seed...+
    sample_quadtree.foreach { edge =>
      quadtree.insert(new QuadRectangle(edge.getEnvelopeInternal), edge)
    }
    quadtree.assignPartitionIds()
    quadtree.assignPartitionLineage()

    implicit val kdtree: KDBTree = new KDBTree(params.maxentries(), params.maxlevel(), envelope_area)
    val sample_kdtree = sample_quadtree.map { edge => (Random.nextInt(), edge) }.sortBy(_._1).map(_._2) // replacement, sample size, random seed...+
    sample_kdtree.foreach { edge =>
      kdtree.insert(edge.getEnvelopeInternal)
    }
    kdtree.assignLeafIds()

    val PARTITION = "K"

    implicit val cells: Map[Int, Cell] = if (PARTITION == "Q") getCells(quadtree) else getCellsKdtree(kdtree)

    val (edgesA, edgesB) = if (PARTITION == "Q") {
      val edgesQA = partitionEdgesByQuadtree(edgesRDDA, quadtree, "A")
      val edgesQB = partitionEdgesByQuadtree(edgesRDDB, quadtree, "B")

      (edgesQA, edgesQB)
    }
    else {
      val edgesAK = partitionEdgesByKdtree(edgesRDDA, kdtree, "A")
      val edgesBK = partitionEdgesByKdtree(edgesRDDB, kdtree, "B")
      (edgesAK, edgesBK)
    }

    save("/tmp/edgesLA.wkt") {
      edgesA.map { edge =>
        val wkt = edge.toText
        val dat = edge.getUserData.asInstanceOf[EdgeData].toString

        s"$wkt\t$dat\n"
      }.collect()
    }

    save("/tmp/edgesLB.wkt") {
      edgesB.map { edge =>
        val wkt = edge.toText
        val dat = edge.getUserData.asInstanceOf[EdgeData].toString

        s"$wkt\t$dat\n"
      }.collect()
    }


    val non_emptiesA = getNonEmptyCells(edgesA)
    val non_emptiesB = getNonEmptyCells(edgesB, "B")

    val ldcelA = createLocalDCELs(edgesA)
    save("/tmp/edgesDA.wkt") {
      ldcelA.map { case (h, l, e, p) =>
        val wkt = p.toText
        val lab = l
        s"$wkt\t$lab\n"
      }.collect()
    }
    val ldcelB = createLocalDCELs(edgesB, "B")
    save("/tmp/edgesDB.wkt") {
      ldcelB.map { case (h, l, e, p) =>
        val wkt = p.toText
        val lab = l
        s"$wkt\t$lab\n"
      }.collect()
    }

    //val ma = runEmptyCells(ldcelA, non_emptiesA)
    //val mb = runEmptyCells(ldcelB, non_emptiesB)

    val mx = Map.empty[String, EmptyCell]

    DCELOverlay2.overlay(ldcelA, mx, ldcelB, mx)


    /*
    val ldcelB = createLocalDCELs(edgesB, "B")
    val mb = runEmptyCells(ldcelB, non_emptiesB, "B")

    DCELOverlay2.overlay(ldcelA, ma, ldcelB, mb)
    */

    spark.close
  }

}
