package edu.ucr.dblab.sdcel.reader

import com.vividsolutions.jts.algorithm.CGAlgorithms
import com.vividsolutions.jts.geom._
import com.vividsolutions.jts.io.WKTReader
import edu.ucr.dblab.sdcel.DCELPartitioner2
import edu.ucr.dblab.sdcel.PartitionReader.envelope2ring
import edu.ucr.dblab.sdcel.Utils.{Settings, log, save}
import edu.ucr.dblab.sdcel.geometries.Cell
import edu.ucr.dblab.sdcel.kdtree.KDBTree
import edu.ucr.dblab.sdcel.quadtree.{QuadRectangle, StandardQuadTree}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters.{asScalaBufferConverter, mapAsScalaMapConverter}

object PR_Utils {
  def read(input: String)(implicit spark: SparkSession, G: GeometryFactory, S: Settings): RDD[LineString] = {

    val polys = spark.read.textFile(input).rdd.cache()
    debug{
      val nPolys = polys.count
      log(s"INFO|npolys=$nPolys")
    }
    val edgesRaw = polys.mapPartitions{ lines =>
      val reader = new WKTReader(G)
      lines.flatMap{ line =>
        val arr = line.split("\t")
        val wkt = arr(0)
        val id  = arr(1).toLong
        val geom = reader.read(wkt)

        geom match {
          case _: Polygon =>
            (0 until geom.getNumGeometries).map { i =>
              (geom.getGeometryN(i).asInstanceOf[Polygon], id)
            }.flatMap { case (polygon, id) =>
              getLineStrings(polygon, id)
            }.toIterator
          case _: MultiPolygon =>
            val polygon = geom.getGeometryN(0).asInstanceOf[Polygon]
            getLineStrings(polygon, id)

          case _: LineString =>
            val coords = geom.getCoordinates
            coords.zip(coords.tail).zipWithIndex.map { case (pair, order) =>
              val coord1 = pair._1
              val coord2 = pair._2
              val coords = Array(coord1, coord2)
              val isHole = false
              val n = geom.getNumPoints - 2

              val line = G.createLineString(coords)
              // Save info from the edge...
              line.setUserData(s"$id\t0\t$order\t$isHole\t$n")

              line
            }
          case _ =>
            println(s"$id\tNot a valid geometry")
            List.empty[LineString]
        }
      }
    }.cache()

    edgesRaw
  }

  private def getLineStrings(polygon: Polygon, polygon_id: Long)(implicit geofactory: GeometryFactory): List[LineString] = {
    getRings(polygon)
      .zipWithIndex
      .flatMap{ case(ring, ring_id) =>
        ring.zip(ring.tail).zipWithIndex.map{ case(pair, order) =>
          val coord1 = pair._1
          val coord2 = pair._2
          val coords = Array(coord1, coord2)
          val isHole = ring_id > 0
          val n = polygon.getNumPoints - 2

          val line = geofactory.createLineString(coords)
          // Save info from the edge...
          line.setUserData(s"$polygon_id\t$ring_id\t$order\t$isHole\t$n")

          line
        }
      }
  }

  private def getRings(polygon: Polygon): List[Array[Coordinate]] = {
    val exterior_coordinates = polygon.getExteriorRing.getCoordinateSequence.toCoordinateArray
    val outerRing = if(!CGAlgorithms.isCCW(exterior_coordinates)) { exterior_coordinates.reverse } else { exterior_coordinates }

    val nInteriorRings = polygon.getNumInteriorRing
    val innerRings = (0 until nInteriorRings).map{ i =>
      val interior_coordinates = polygon.getInteriorRingN(i).getCoordinateSequence.toCoordinateArray
      if(CGAlgorithms.isCCW(interior_coordinates)) { interior_coordinates.reverse } else { interior_coordinates }
    }.toList

    outerRing +: innerRings
  }

  def getStudyArea(edges: RDD[LineString]): Envelope = {
    edges.map(_.getEnvelopeInternal).reduce { (a, b) =>
      a.expandToInclude(b)
      a
    }
  }

  def getCells(quadtree: StandardQuadTree[LineString])(implicit G: GeometryFactory, S: Settings): Map[Int, Cell] = {
    val cells = quadtree.getLeafZones.asScala.map { leaf =>
      val id = leaf.partitionId.toInt
      val lineage = leaf.lineage
      val envelope = leaf.getEnvelope
      val mbr = envelope2ring(envelope)

      val cell = Cell(id, lineage, mbr)
      id -> cell
    }.toMap

    debug{
      save("/tmp/edgesCells.wkt"){
        cells.values.toList.map(_.wkt + "\n")
      }
    }

    cells
  }

  def getCellsKdtree(kdtree: KDBTree)(implicit G: GeometryFactory, S: Settings): Map[Int, Cell] = {
    val cells = kdtree.getLeaves.asScala.map { case(id, envelope) =>
      val mbr = envelope2ring(envelope)

      val cell = Cell(id, "", mbr)
      id.toInt -> cell
    }.toMap

    debug{
      save("/tmp/edgesCells.wkt"){
        cells.values.toList.map(_.wkt + "\n")
      }
    }

    cells
  }

  def partitionEdgesByQuadtree(edgesRDD: RDD[LineString], quadtree: StandardQuadTree[LineString], label: String)
                    (implicit spark: SparkSession, G: GeometryFactory, cells: Map[Int, Cell]): RDD[LineString] = {
    val n = quadtree.getLeafZones.size()
    val partitioner = new edu.ucr.dblab.sdcel.SimplePartitioner[LineString](n)

    val edges = edgesRDD.mapPartitions{ edges =>
      edges.flatMap { edge =>
        val rectangle = new QuadRectangle(edge.getEnvelopeInternal)
        quadtree.findZones(rectangle).asScala.map{ zone =>
          (zone.partitionId, edge)
        }
      }
    }.partitionBy(partitioner).map{_._2}

    DCELPartitioner2.getEdgesWithCrossingInfo(edges, cells, label)
  }
  def partitionEdgesByKdtree(edgesRDD: RDD[LineString], kdtree: KDBTree, label: String)
                              (implicit spark: SparkSession, G: GeometryFactory, cells: Map[Int, Cell]): RDD[LineString] = {
    val n = kdtree.getLeaves.size()
    val partitioner = new edu.ucr.dblab.sdcel.SimplePartitioner[LineString](n)

    val edges = edgesRDD.mapPartitions{ edges =>
      edges.flatMap { edge =>
        val envelope = edge.getEnvelopeInternal
        kdtree.findLeafNodes(envelope).asScala.map{ node =>
          (node.getLeafId, edge)
        }
      }
    }.partitionBy(partitioner).map{_._2}

    DCELPartitioner2.getEdgesWithCrossingInfo(edges, cells, label)
  }

  def saveEdgesRDD(path: String, edgesRDD: RDD[LineString]): Unit = {
    save(path) {
      edgesRDD.mapPartitionsWithIndex { (cid, edges) =>
        edges.map { edge =>
          val wkt = edge.toText
          val dat = edge.getUserData

          s"$wkt\t$cid\t$dat\n"
        }
      }.collect
    }
  }

  def debug[R](block: => R)(implicit S: Settings): Unit = { if(S.debug) block }

}
