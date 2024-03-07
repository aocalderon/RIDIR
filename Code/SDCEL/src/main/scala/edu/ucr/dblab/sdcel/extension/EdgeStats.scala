package edu.ucr.dblab.sdcel.extension

import com.vividsolutions.jts.geom.{Envelope, GeometryFactory, LineString, PrecisionModel}
import edu.ucr.dblab.sdcel.{DCELPartitioner2, Params, SimplePartitioner}
import edu.ucr.dblab.sdcel.Utils.{Settings, log, timer}
import edu.ucr.dblab.sdcel.kdtree.KDBTree
import edu.ucr.dblab.sdcel.quadtree.{QuadRectangle, StandardQuadTree}
import edu.ucr.dblab.sdcel.reader.PR_Utils.{getCells, getCellsKdtree, getStudyArea, read}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.util.Random

object EdgeStats {
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

    // Reading data...
    val edgesRDDA = read(params.input1()).cache
    val nEdgesRDDA = edgesRDDA.count()
    log(s"INFO|$tag|edgesA|$nEdgesRDDA")
    val edgesRDDB = read(params.input2()).cache
    val nEdgesRDDB = edgesRDDB.count()
    log(s"INFO|$tag|edgesB|$nEdgesRDDB")
    val edgesRDD = edgesRDDA.union(edgesRDDB).cache()
    val nEdgesRDD = edgesRDD.count()
    log(s"INFO|$tag|TotalEdges|$nEdgesRDD")

    // Collecting statistics for partitioning...
    val envelope_area = getStudyArea(edgesRDD) // getStudyArea returns an Envelope...
    val paddedBoundary = new Envelope(envelope_area.getMinX, envelope_area.getMaxX + 0.01, envelope_area.getMinY, envelope_area.getMaxY + 0.01)
    val numPartitions = params.partitions()
    log(s"INFO|$tag|Requested_partitions|$numPartitions")
    val sample_size = SampleUtils.getSampleNumbers(numPartitions, nEdgesRDD)
    log(s"INFO|$tag|Sample_size|$sample_size")
    val fraction = SampleUtils.computeFractionForSampleSize(sample_size, nEdgesRDD, withReplacement = false)
    log(s"INFO|$tag|Fraction|$fraction")

    val LA = edgesRDDA.map{_.getLength}.cache()
    val nLA = LA.count()
    log(s"STAT|A|Both|$numPartitions|$tag|Count|$nLA")
    val avgLenA = LA.sum() / nLA
    log(s"STAT|A|Both|$numPartitions|$tag|Length|$avgLenA")

    val LB = edgesRDDB.map{_.getLength}.cache()
    val nLB = LB.count()
    log(s"STAT|B|Both|$numPartitions|$tag|Count|$nLB")
    val avgLenB = LB.sum() / nLB
    log(s"STAT|B|Both|$numPartitions|$tag|Length|$avgLenB")

    /** **
     * Testing Kdtree...
     * ** */
    val ((kdtree, kdtree_space), kdtree_creation_time) = timer {
      val sample = edgesRDD.sample(withReplacement = false, fraction, 42)
        .map(sample => (Random.nextDouble(), sample)).sortBy(_._1).map(_._2)
        .collect()

      val max_items_per_cell = sample.length / numPartitions
      log(s"INFO|$tag|Kdtree|maxItemsPerCell|$max_items_per_cell")

      val kdtree = new KDBTree(max_items_per_cell, numPartitions, paddedBoundary)
      sample.foreach { sample =>
        kdtree.insert(sample.getEnvelopeInternal)
      }
      kdtree.assignLeafIds()
      val kn = kdtree.getLeaves.size()

      kdtree.dropElements()

      (kdtree, kn)
    }
    log(s"TIME|$tag|$numPartitions|Kdtree|creation|$kdtree_creation_time")
    log(s"INFO|$tag|$numPartitions|Kdtree|space|$kdtree_space")

    val ( (nKA, nKB), kdtree_partitioning_time) = timer {
      val edgesA = edgesRDDA.mapPartitions { edges =>
        edges.flatMap { edge =>
          kdtree.findLeafNodes(edge.getEnvelopeInternal).asScala.map { leaf =>
            (leaf.getLeafId, edge)
          }
        }
      }.partitionBy(new SimplePartitioner[LineString](kdtree_space)).map(_._2)
      val edgesB = edgesRDDB.mapPartitions { edges =>
        edges.flatMap { edge =>
          kdtree.findLeafNodes(edge.getEnvelopeInternal).asScala.map { leaf =>
            (leaf.getLeafId, edge)
          }
        }
      }.partitionBy(new SimplePartitioner[LineString](kdtree_space)).map(_._2)

      val na = edgesA.count()
      val nb = edgesB.count()

      (na, nb)
    }
    log(s"TIME|$tag|$numPartitions|Kdtree|partitioning|$kdtree_partitioning_time")
    log(s"STAT|A|Kdtree|$numPartitions|$tag|Replication|$nKA")
    log(s"STAT|B|Kdtree|$numPartitions|$tag|Replication|$nKB")

    /** **
     * Testing Quadtree...
     * ** */
    val ((quadtree, quadtree_space), quadtree_creation_time) = timer {
      val sample = edgesRDD.sample(withReplacement = false, fraction, 42).collect()
      val max_items_per_cell = sample.length / numPartitions
      log(s"INFO|$tag|Quadtree|$numPartitions|maxItemsPerCell|$max_items_per_cell")
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
    log(s"TIME|$tag|$numPartitions|Quadtree|creation|$quadtree_creation_time")
    log(s"INFO|$tag|$numPartitions|Quadtree|space|$quadtree_space")

    val ( (nQA, nQB), quadtree_partitioning_time) = timer {
      val edgesA = edgesRDDA.mapPartitions { edges =>
        edges.flatMap { edge =>
          quadtree.findZones(new QuadRectangle(edge.getEnvelopeInternal)).asScala.map { leaf =>
            (leaf.partitionId, edge)
          }
        }
      }.partitionBy(new SimplePartitioner[LineString](quadtree_space)).map(_._2)
      val edgesB = edgesRDDB.mapPartitions { edges =>
        edges.flatMap { edge =>
          quadtree.findZones(new QuadRectangle(edge.getEnvelopeInternal)).asScala.map { leaf =>
            (leaf.partitionId, edge)
          }
        }
      }.partitionBy(new SimplePartitioner[LineString](quadtree_space)).map(_._2)

      val na = edgesA.count()
      val nb = edgesB.count()

      (na, nb)
    }
    log(s"TIME|$tag|$numPartitions|Quadtree|partitioning|$quadtree_partitioning_time")
    log(s"STAT|A|Quadtree|$numPartitions|$tag|Replication|$nQA")
    log(s"STAT|B|Quadtree|$numPartitions|$tag|Replication|$nQB")


    log("TIME|End")
    spark.close()
  }


}
