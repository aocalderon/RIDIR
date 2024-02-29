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

    log("TIME|Start")

    log(s"INFO|scale|${S.scale}")
    implicit val model: PrecisionModel = new PrecisionModel(S.scale)
    implicit val G: GeometryFactory = new GeometryFactory(model)

    // Reading data...
    val edgesRDDA = read(params.input1())
    val nEdgesRDDA = edgesRDDA.count()
    log(s"INFO|edgesA|$nEdgesRDDA")
    val edgesRDDB = read(params.input2())
    val nEdgesRDDB = edgesRDDB.count()
    log(s"INFO|edgesB|$nEdgesRDDB")
    val edgesRDD = edgesRDDA.union(edgesRDDB).cache()
    val nEdgesRDD = edgesRDD.count()
    log(s"INFO|TotalEdges|$nEdgesRDD")

    // Collecting statistics for partitioning...
    val envelope_area = getStudyArea(edgesRDD) // getStudyArea returns an Envelope...
    val paddedBoundary = new Envelope(envelope_area.getMinX, envelope_area.getMaxX + 0.01, envelope_area.getMinY, envelope_area.getMaxY + 0.01)
    val numPartitions = params.partitions()
    log(s"INFO|Requested_partitions|$numPartitions")
    val sample_size = SampleUtils.getSampleNumbers(numPartitions, nEdgesRDD)
    log(s"INFO|Sample_size|$sample_size")
    val fraction = SampleUtils.computeFractionForSampleSize(sample_size, nEdgesRDD, withReplacement = false)
    log(s"INFO|Fraction|$fraction")

    /** **
     * Testing Kdtree...
     * ** */
    val ((kdtree, kdtree_space), kdtree_creation_time) = timer {
      val sample = edgesRDD.sample(withReplacement = false, fraction, 42)
        .map(sample => (Random.nextDouble(), sample)).sortBy(_._1).map(_._2)
        .collect()

      val max_items_per_cell = sample.length / numPartitions
      log(s"INFO|Kdtree|maxItemsPerCell|$max_items_per_cell")

      val kdtree = new KDBTree(max_items_per_cell, numPartitions, paddedBoundary)
      sample.foreach { sample =>
        kdtree.insert(sample.getEnvelopeInternal)
      }
      kdtree.assignLeafIds()
      val kn = kdtree.getLeaves.size()

      kdtree.dropElements()

      (kdtree, kn)
    }
    log(s"TIME|$numPartitions|Kdtree|creation|$kdtree_creation_time")
    log(s"INFO|$numPartitions|Kdtree|space|$kdtree_space")

    val ( (edgesKA, edgesKB, kcells), kdtree_partitioning_time) = timer {
      val edgesPartitionedRDDA = edgesRDDA.mapPartitions { edges =>
        edges.flatMap { edge =>
          kdtree.findLeafNodes(edge.getEnvelopeInternal).asScala.map { leaf =>
            (leaf.getLeafId, edge)
          }
        }
      }.partitionBy(new SimplePartitioner[LineString](kdtree_space)).map(_._2)
      val edgesPartitionedRDDB = edgesRDDB.mapPartitions { edges =>
        edges.flatMap { edge =>
          kdtree.findLeafNodes(edge.getEnvelopeInternal).asScala.map { leaf =>
            (leaf.getLeafId, edge)
          }
        }
      }.partitionBy(new SimplePartitioner[LineString](kdtree_space)).map(_._2)

      val kcells = getCellsKdtree(kdtree)
      val edgesA = DCELPartitioner2.getEdgesWithCrossingInfo(edgesPartitionedRDDA, kcells, "A").cache()
      val edgesB = DCELPartitioner2.getEdgesWithCrossingInfo(edgesPartitionedRDDB, kcells, "B").cache()

      (edgesA, edgesB, kcells)
    }
    log(s"TIME|$numPartitions|Kdtree|partitioning|$kdtree_partitioning_time")

    val (kdtree_overlay, kdtree_overlay_time) = timer {
      implicit val cells: Map[Int, Cell] = kcells
      val ldcelA = createLocalDCELs(edgesKA)
      val ldcelB = createLocalDCELs(edgesKB)
      val mx = Map.empty[String, EmptyCell]

      val overlay = DCELOverlay2.overlay(ldcelA, mx, ldcelB, mx).cache()
      overlay.count()

      overlay
    }
    log(s"TIME|$numPartitions|Kdtree|overlay|$kdtree_overlay_time")

    save(params.kpath()) {
      kdtree.getLeaves.asScala.map { case (id, envelope) =>
        val wkt = G.toGeometry(envelope)
        s"$wkt\t$id\n"
      }.toList
    }
    save("/tmp/edgesKO.wkt") {
      kdtree_overlay.map { case (face, label) =>
        val wkt = face.toText

        s"$wkt\t$label\n"
      }.collect()
    }

    /** **
     * Testing Quadtree...
     * ** */
    val ((quadtree, quadtree_space), quadtree_creation_time) = timer {
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
      quadtree.dropElements()

      (quadtree, qn)
    }
    log(s"TIME|$numPartitions|Quadtree|creation|$quadtree_creation_time")
    log(s"INFO|$numPartitions|Quadtree|space|$quadtree_space")

    val ( (edgesQA, edgesQB, qcells), quadtree_partitioning_time) = timer {
      val edgesPartitionedRDDA = edgesRDDA.mapPartitions { edges =>
        edges.flatMap { edge =>
          quadtree.findZones(new QuadRectangle(edge.getEnvelopeInternal)).asScala.map { leaf =>
            (leaf.partitionId, edge)
          }
        }
      }.partitionBy(new SimplePartitioner[LineString](quadtree_space)).map(_._2)
      val edgesPartitionedRDDB = edgesRDDB.mapPartitions { edges =>
        edges.flatMap { edge =>
          quadtree.findZones(new QuadRectangle(edge.getEnvelopeInternal)).asScala.map { leaf =>
            (leaf.partitionId, edge)
          }
        }
      }.partitionBy(new SimplePartitioner[LineString](quadtree_space)).map(_._2)

      val qcells = getCells(quadtree)
      val edgesA = DCELPartitioner2.getEdgesWithCrossingInfo(edgesPartitionedRDDA, qcells, "A").cache()
      val edgesB = DCELPartitioner2.getEdgesWithCrossingInfo(edgesPartitionedRDDB, qcells, "B").cache()

      (edgesA, edgesB, qcells)
    }
    log(s"TIME|$numPartitions|Quadtree|partitioning|$quadtree_partitioning_time")

    val (quadtree_overlay, quadtree_overlay_time) = timer{
      implicit val cells: Map[Int, Cell] = qcells
      val ldcelA = createLocalDCELs(edgesQA)
      val ldcelB = createLocalDCELs(edgesQB)
      val mx = Map.empty[String, EmptyCell]

      val overlay = DCELOverlay2.overlay(ldcelA, mx, ldcelB, mx).cache()
      overlay.count()

      overlay
    }
    log(s"TIME|$numPartitions|Quadtree|overlay|$quadtree_overlay_time")

    save(params.qpath()) {
      quadtree.getLeafZones.asScala.map { zone =>
        val wkt = G.toGeometry(zone.getEnvelope)
        val id = zone.partitionId

        s"$wkt\t$id\n"
      }.toList
    }
    save("/tmp/edgesQO.wkt") {
      quadtree_overlay.map { case (face, label) =>
        val wkt = face.toText

        s"$wkt\t$label\n"
      }.collect()
    }

    spark.close
  }
}
