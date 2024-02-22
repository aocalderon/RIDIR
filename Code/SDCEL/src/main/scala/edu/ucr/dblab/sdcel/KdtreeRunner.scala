package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geom.{Coordinate, Envelope, GeometryFactory, LineString, PrecisionModel}
import edu.ucr.dblab.sdcel.Utils.{Settings, log, save}
import edu.ucr.dblab.sdcel.kdtree.KDBTree
import edu.ucr.dblab.sdcel.kdtree.KDBTree.Visitor
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator

import scala.collection.JavaConverters.{asScalaBufferConverter, mapAsScalaMapConverter}
import scala.collection.mutable
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

    val MAX_LEVEL=10
    val CAPACITY=100
    val FRACTION=0.1

    val d = 10.0
    val W = 2000
    val H = 2000
    val n = 50000
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
    save("/tmp/edgesCells.wkt") {
      lines_tree.getLeaves.asScala.map { case (id, envelope) =>
        val wkt = G.toGeometry(envelope).toText
        s"$wkt\t$id\n"
      }.toList
    }
    val matches: mutable.HashMap[Integer, List[Envelope]] = new mutable.HashMap[Integer, List[Envelope]]()
    lines_tree.traverse( new Visitor {
      override def visit(tree: KDBTree): Boolean = {
        if (tree.isLeaf) matches.put(tree.getLeafId, tree.getItems.asScala.toList)
        true
      }
    })
    save("/tmp/edgesEnvelopes.wkt") {
      matches.flatMap { case (id, envelopes) =>
        envelopes.map { envelope =>
          val wkt = G.toGeometry(envelope).toText
          s"$wkt\t$id\n"
        }
      }.toList
    }

    lines_tree.dropElements()
    val partitioner = new edu.ucr.dblab.sdcel.SimplePartitioner[LineString](lines_tree.getLeaves.size())

    val linesPartitionedRDD = linesRDD.mapPartitionsWithIndex{ (index, lines) =>
      lines.flatMap{ line =>
        val envelope = line.getEnvelopeInternal
        lines_tree.findLeafNodes(envelope).asScala.map{ leaf =>
          val nid = leaf.getLeafId
          //val lid = line.getUserData.asInstanceOf[String]
          //val data = s"$lid\t$index"
          //line.setUserData(data)
          (nid, line)
        }
      }
    }.partitionBy(partitioner).map(_._2).cache()

    save("/tmp/edgesOrder.wkt") {
      linesPartitionedRDD.mapPartitionsWithIndex { (cid, lines) =>
        lines.zipWithIndex.map { case (line, order) =>
          val wkt = line.toText
          val dat = line.getUserData.asInstanceOf[String]

          s"$wkt\t$order\t$dat\t$cid\n"
        }
      }.collect()
    }

    spark.close
  }
}
