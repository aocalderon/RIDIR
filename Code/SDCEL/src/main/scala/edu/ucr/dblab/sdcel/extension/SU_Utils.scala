package edu.ucr.dblab.sdcel.extension

import com.vividsolutions.jts.geom.{Coordinate, Envelope, Geometry, GeometryFactory, LinearRing, Polygon}
import com.vividsolutions.jts.index.strtree.STRtree
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.rdd.RDD

import scala.io.Source

object SU_Utils {
  def getEnvelope(data: RDD[Geometry]): Envelope = {
    data.map{_.getEnvelopeInternal}.reduce{ (a, b) =>
      a.expandToInclude(b)
      a
    }
  }

  def gridPartitions(grids_file: String)(implicit G: GeometryFactory): (Map[Int, Polygon], STRtree) = {
    val reader = new WKTReader(G)
    val buffer = Source.fromFile(grids_file)
    val tree   = new STRtree()
    val lines  = buffer.getLines().toList
    val n = lines.size
    val grids = lines.map{ line =>
      val arr = line.split("\t")
      val mbr = reader.read(arr(0)).asInstanceOf[Polygon]
      val  id = arr(1).toInt
      tree.insert(mbr.getEnvelopeInternal, id)
      id -> mbr
    }.toMap
    buffer.close()

    (grids, tree)
  }
  def envelope2ring(e: Envelope)(implicit geofactory: GeometryFactory): LinearRing = {

    val W = e.getMinX
    val S = e.getMinY
    val E = e.getMaxX
    val N = e.getMaxY
    val WS = new Coordinate(W, S)
    val ES = new Coordinate(E, S)
    val EN = new Coordinate(E, N)
    val WN = new Coordinate(W, N)
    geofactory.createLinearRing(Array(WS,ES,EN,WN,WS))
  }
}
