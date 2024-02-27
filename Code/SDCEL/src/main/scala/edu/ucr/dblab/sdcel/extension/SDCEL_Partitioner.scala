package edu.ucr.dblab.sdcel.extension

import com.vividsolutions.jts.geom.{Envelope, GeometryFactory, LineString, PrecisionModel}
import edu.ucr.dblab.sdcel.LocalDCEL.createLocalDCELs
import edu.ucr.dblab.sdcel.Utils.{Settings, log, save, timer}
import edu.ucr.dblab.sdcel.cells.EmptyCellManager2.EmptyCell
import edu.ucr.dblab.sdcel.geometries.Cell
import edu.ucr.dblab.sdcel.kdtree.KDBTree
import edu.ucr.dblab.sdcel.quadtree.{QuadRectangle, StandardQuadTree}
import edu.ucr.dblab.sdcel.reader.PR_Utils._
import edu.ucr.dblab.sdcel.{DCELOverlay2, DCELPartitioner2, Params, SimplePartitioner}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters.{asScalaBufferConverter, mapAsScalaMapConverter}
import scala.util.Random

object SDCEL_Partitioner {
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
    val nEdgesRDD = edgesRDD.count()
    log(s"INFO|TotalEdges=$nEdgesRDD")

    val envelope_area = getStudyArea(edgesRDD) // getStudyArea returns an Envelope...
    val paddedBoundary = new Envelope(envelope_area.getMinX, envelope_area.getMaxX + 0.01, envelope_area.getMinY, envelope_area.getMaxY + 0.01)
    val numPartitions = params.partitions()

    log(s"INFO|Input_partitions|$numPartitions")

    val sample_size = SampleUtils.getSampleNumbers(numPartitions, nEdgesRDD)

    log(s"INFO|Sample_size|$sample_size")

    val fraction = SampleUtils.computeFractionForSampleSize(sample_size, nEdgesRDD, withReplacement = false)

    log(s"INFO|Fraction|$fraction")

    /****
     * Testing Kdtree...
     ****/
    val ( (edgesKA, edgesKB, kdtree, kcells, koverlay), kdtree_time) = timer{
      val sample = edgesRDD.sample(withReplacement = false, fraction, 42)
        .map(sample => (Random.nextDouble(), sample)).sortBy(_._1).map(_._2).collect()

      val max_items_per_cell = sample.length / numPartitions
      log(s"INFO|Kdtree|$numPartitions|maxItemsPerCell|$max_items_per_cell")

      val kdtree = new KDBTree(max_items_per_cell, numPartitions, paddedBoundary)
      sample.foreach { sample =>
        kdtree.insert(sample.getEnvelopeInternal)
      }
      kdtree.assignLeafIds()
      val kn = kdtree.getLeaves.size()
      log(s"INFO|Kdtree|$numPartitions|numPartitions|$kn")

      kdtree.dropElements()
      val edgesPartitionedRDDA = edgesRDDA.mapPartitions{ edges =>
        edges.flatMap{ edge =>
          kdtree.findLeafNodes(edge.getEnvelopeInternal).asScala.map{ leaf =>
            (leaf.getLeafId, edge)
          }
        }
      }.partitionBy(new SimplePartitioner[LineString](kn)).map(_._2)
      val edgesPartitionedRDDB = edgesRDDB.mapPartitions{ edges =>
        edges.flatMap{ edge =>
          kdtree.findLeafNodes(edge.getEnvelopeInternal).asScala.map{ leaf =>
            (leaf.getLeafId, edge)
          }
        }
      }.partitionBy(new SimplePartitioner[LineString](kn)).map(_._2)

      val kcells = getCellsKdtree(kdtree)
      val edgesA = DCELPartitioner2.getEdgesWithCrossingInfo(edgesPartitionedRDDA, kcells, "A").cache()
      val edgesB = DCELPartitioner2.getEdgesWithCrossingInfo(edgesPartitionedRDDB, kcells, "B").cache()

      implicit val cells: Map[Int, Cell] = kcells
      val ldcelA = createLocalDCELs(edgesA)
      val ldcelB = createLocalDCELs(edgesB)
      val mx = Map.empty[String, EmptyCell]

      val overlay = DCELOverlay2.overlay(ldcelA, mx, ldcelB, mx).cache()
      overlay.count()

      (edgesA, edgesB, kdtree, cells, overlay)
    }
    edgesKA.cache
    edgesKA.count
    edgesKB.cache
    edgesKB.count
    val kdtree_space = kcells.size
    log(s"TIME|Kdtree|$numPartitions|$kdtree_space|$kdtree_time")

    save(params.cpath()) {
      kdtree.getLeaves.asScala.map { case (id, envelope) =>
        val wkt = G.toGeometry(envelope)
        s"$wkt\t$id\n"
      }.toList
    }
    saveEdgesRDD("/tmp/edgesKA.wkt", edgesKA)
    saveEdgesRDD("/tmp/edgesKB.wkt", edgesKB)
    save("/tmp/edgesKO.wkt"){
      koverlay.map{ case(face, label) =>
        val wkt = face.toText

        s"$wkt\t$label\n"
      }.collect()
    }

    /****
     * Testing Quadtree...
     ****/
    val ( (edgesQA, edgesQB, quadtree, qcells, qoverlay), quadtree_time) = timer {
      val sample = edgesRDD.sample(withReplacement = false, fraction, 42).collect()
      val max_items_per_cell = sample.length / numPartitions
      log(s"INFO|Quadtree|$numPartitions|maxItemsPerCell|$max_items_per_cell")
      val quadtree = new StandardQuadTree[LineString](new QuadRectangle(paddedBoundary), 0, max_items_per_cell, numPartitions)
      sample.foreach { edge =>
        quadtree.insert(new QuadRectangle(edge.getEnvelopeInternal), edge)
      }
      quadtree.assignPartitionIds()
      quadtree.assignPartitionLineage()
      val qn = quadtree.getLeafZones.size()

      log(s"INFO|Quadtree|$numPartitions|numPartitions|$qn")

      quadtree.dropElements()
      val edgesPartitionedRDDA = edgesRDDA.mapPartitions{ edges =>
        edges.flatMap{ edge =>
          quadtree.findZones(new QuadRectangle(edge.getEnvelopeInternal)).asScala.map{ leaf =>
            (leaf.partitionId, edge)
          }
        }
      }.partitionBy(new SimplePartitioner[LineString](qn)).map(_._2)
      val edgesPartitionedRDDB = edgesRDDB.mapPartitions{ edges =>
        edges.flatMap{ edge =>
          quadtree.findZones(new QuadRectangle(edge.getEnvelopeInternal)).asScala.map{ leaf =>
            (leaf.partitionId, edge)
          }
        }
      }.partitionBy(new SimplePartitioner[LineString](qn)).map(_._2)

      val qcells = getCells(quadtree)
      val edgesA = DCELPartitioner2.getEdgesWithCrossingInfo(edgesPartitionedRDDA, qcells, "A").cache()
      val edgesB = DCELPartitioner2.getEdgesWithCrossingInfo(edgesPartitionedRDDB, qcells, "B").cache()

      implicit val cells: Map[Int, Cell] = qcells
      val ldcelA = createLocalDCELs(edgesA)
      val ldcelB = createLocalDCELs(edgesB)
      val mx = Map.empty[String, EmptyCell]

      val overlay = DCELOverlay2.overlay(ldcelA, mx, ldcelB, mx).cache()
      overlay.count()

      (edgesA, edgesB, quadtree, cells, overlay)
    }
    edgesQA.cache
    edgesQA.count
    edgesQB.cache
    edgesQB.count
    val quadtree_space = qcells.size
    log(s"TIME|Quadtree|$numPartitions|$quadtree_space|$quadtree_time")

    save(params.qpath()) {
      quadtree.getLeafZones.asScala.map { zone =>
        val wkt = G.toGeometry(zone.getEnvelope)
        val  id = zone.partitionId

        s"$wkt\t$id\n"
      }.toList
    }
    saveEdgesRDD("/tmp/edgesQA.wkt", edgesQA)
    saveEdgesRDD("/tmp/edgesQB.wkt", edgesQB)
    save("/tmp/edgesQO.wkt"){
      qoverlay.map{ case(face, label) =>
        val wkt = face.toText

        s"$wkt\t$label\n"
      }.collect()
    }

    spark.close
  }
}
