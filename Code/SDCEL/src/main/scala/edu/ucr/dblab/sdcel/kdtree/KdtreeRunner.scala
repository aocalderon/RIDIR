package edu.ucr.dblab.sdcel.kdtree

import com.vividsolutions.jts.geom._
import edu.ucr.dblab.sdcel.Params
import edu.ucr.dblab.sdcel.Utils.{Settings, log, save}
import edu.ucr.dblab.sdcel.kdtree.KDBTree_Utils.{getAllCells, getLineages}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator

import scala.annotation.tailrec
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

object KdtreeRunner {
  def main(args: Array[String]): Unit = {
    // Starting session...
    implicit val spark: SparkSession = SparkSession.builder()
      .master("local[3]")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()
    implicit val params: Params = new Params(args)
    implicit val S: Settings = Settings(
      tolerance = params.tolerance(),
      debug = params.debug(),
      appId = spark.sparkContext.applicationId
    )
    val command = System.getProperty("sun.java.command")
    log(s"COMMAND|$command")
    log(s"INFO|scale=${S.scale}")
    implicit val M: PrecisionModel = new PrecisionModel(S.scale)
    implicit val G: GeometryFactory = new GeometryFactory(M)
    log("TIME|Start")

    val MAX_LEVEL=15
    val CAPACITY=25
    val FRACTION=0.25

    val d = 10.0
    val W = 200
    val H = 200
    val n = 20000
    val lines = for(i <- 0 until n) yield {
      val x1 = Random.nextInt(W)
      val y1 = Random.nextInt(H)
      val dx = Random.nextDouble() * d
      val dy = Random.nextDouble() * d
      val xsig = if(Random.nextInt(2) == 0) -1.0 else 1.0
      val ysig = if(Random.nextInt(2) == 0) -1.0 else 1.0
      val x2 = x1 + (dx * xsig)
      val y2 = y1 + (dy * ysig)
      val coords = Array(new Coordinate(x1,y1), new Coordinate(x2,y2))
      val p = G.createLineString(coords)
      p.setUserData(i.toString)
      p
    }
    save("/tmp/edgesLines.wkt"){
      lines.map{ line =>
        val wkt = line.toText
        val  id = line.getUserData.asInstanceOf[String]

        s"$wkt\t$id\n"
      }
    }
    val linesRDD = spark.sparkContext.parallelize(lines)
    val lines_envelope = new Envelope(0 - d, W + d, 0 - d, H + d)
    val lines_tree: KDBTree = new KDBTree(CAPACITY, MAX_LEVEL, lines_envelope)
    val sample_lines = linesRDD.sample(withReplacement = false, FRACTION, 42L).collect()
    sample_lines.foreach{ lines =>
      lines_tree.insert(lines.getEnvelopeInternal)
    }
    lines_tree.assignLeafIds()
    lines_tree.assignPartitionLineage()
    lines_tree.dropElements()

    val kn = lines_tree.getLeaves.size()
    val kcells = getAllCells(lines_tree)

    save("/tmp/edgesCells.wkt") {
      kcells.map { case(_, node) =>
        val wkt = G.toGeometry(node.envelope).toText
        val lin = node.lineage
        val spx = node.splitX

        s"$wkt\tL${lin}_$spx\n"
      }.toList
    }

    val lineages = getLineages(lines_tree)
    val x = Random.nextInt(kn)
    var x_lineage = lineages(x)

    val nodes = new ListBuffer[(KDBTree, String)]()
    var i: Int = 0
    while(x_lineage.nonEmpty) {
      val x_parent = x_lineage.substring(0, x_lineage.length - 1)
      val x_position = x_lineage.takeRight(1)
      val sibling_position = x_position == "0"
      val x_sibling = s"$x_parent${if(sibling_position) "1" else "0"}"
      val kcell = kcells(x_sibling)
      val sibling_orientation = kcell.splitX

      val sibling_envelope = kcells(x_sibling).envelope
      sibling_envelope.expandBy(S.tolerance * -1.0)
      val nodes_presorted = lines_tree.findLeafNodes(sibling_envelope).asScala.toList
      val nodes_sorted = (sibling_orientation, sibling_position) match {
        case (true, true)   => nodes_presorted.sortBy(_.getExtent.getMinX)
        case (true, false)  => nodes_presorted.sortBy(_.getExtent.getMaxX)(Ordering[Double].reverse)
        case (false, true)  => nodes_presorted.sortBy(_.getExtent.getMinY)
        case (false, false) => nodes_presorted.sortBy(_.getExtent.getMaxY)(Ordering[Double].reverse)
      }

      val nodes_prime = nodes_sorted.zipWithIndex.map{ case(node, index) =>
        (node, s"$i")
      }
      nodes.appendAll(nodes_prime)

      i = i + 1
      x_lineage = x_parent
    }

    @tailrec
    def find_siblings_cells(lineage: String,
                            tree: KDBTree,
                            kcells: mutable.HashMap[String, KDBTree_Utils.Kdbnode],
                            r: List[KDBTree],
                            i: Int): List[KDBTree] = {
      if(lineage.isEmpty){
        r
      } else {
        val parent = lineage.substring(0, lineage.length - 1)
        val position = lineage.takeRight(1)
        val sibling_position = position == "0"
        val sibling = s"$parent${if (sibling_position) "1" else "0"}"
        val kcell = kcells(sibling)
        val sibling_orientation = kcell.splitX

        val sibling_envelope = kcells(sibling).envelope
        sibling_envelope.expandBy(S.tolerance * -1.0)
        val nodes_presorted = tree.findLeafNodes(sibling_envelope).asScala.toList
        val nodes_sorted = (sibling_orientation, sibling_position) match {
          case (true, true)   => nodes_presorted.sortBy(_.getExtent.getMinX)
          case (true, false)  => nodes_presorted.sortBy(_.getExtent.getMaxX)(Ordering[Double].reverse)
          case (false, true)  => nodes_presorted.sortBy(_.getExtent.getMinY)
          case (false, false) => nodes_presorted.sortBy(_.getExtent.getMaxY)(Ordering[Double].reverse)
        }

        find_siblings_cells(parent, tree, kcells, r ++ nodes_sorted, i + 1)
      }
    }
    val nodes_order = find_siblings_cells(lineages(x), lines_tree, kcells, List.empty[KDBTree], 0)

    save("/tmp/edgesS.wkt") {
      nodes_order.zipWithIndex.map { case(leaf, i) =>
        val wkt = G.toGeometry(leaf.getExtent).toText
        val lin = leaf.lineage
        val cid = leaf.getLeafId
        s"$wkt\tL$lin\t$cid\t$i\n"
      }
    }

    log("TIME|End")
    spark.close
  }
}
