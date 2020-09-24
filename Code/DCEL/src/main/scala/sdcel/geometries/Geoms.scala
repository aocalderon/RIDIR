package edu.ucr.dblab.sdcel.geometries

import scala.collection.mutable.ArrayBuffer
import com.vividsolutions.jts.geom.{GeometryFactory, Coordinate}
import com.vividsolutions.jts.geom.{MultiPolygon, Polygon, LineString, Point}

case class EdgeData(polygonId: String, ringId: Int, edgeId: Int, isHole: Boolean){
  override def toString: String = s"$polygonId\t$ringId\t$edgeId\t$isHole"
}

case class Half_edge(edge: LineString) {
  private val coords = edge.getCoordinates
  val v1 = coords(0)
  val v2 = coords(1)
  val orig = Vertex(v1)
  val dest = Vertex(v2)
  val data = edge.getUserData.asInstanceOf[EdgeData]
  var twin: Half_edge = null
  var next: Half_edge = null
  var prev: Half_edge = null

  val angleAtOrig = math.toDegrees(hangle(v1.x - v2.x, v1.y - v2.y))
  val angleAtDest = math.toDegrees(hangle(v2.x - v1.x, v2.y - v1.y))

  private def hangle(dx: Double, dy: Double): Double = {
    val length = math.sqrt( (dx * dx) + (dy * dy) )
    if(dy > 0){
      math.acos(dx / length)
    } else {
      2 * math.Pi - math.acos(dx / length)
    }
  }

  def reverse(implicit geofactory: GeometryFactory): Half_edge = {
    val edge = geofactory.createLineString(Array(v2, v1))
    edge.setUserData(EdgeData("F",0,0,false))
    Half_edge(edge)
  }
}

case class Vertex(coord: Coordinate) {
  private var hedges: ArrayBuffer[Half_edge] = new ArrayBuffer[Half_edge]()
  val x = coord.x
  val y = coord.y

  def hedgesNum: Int = hedges.size

  def setHedges(hedges: List[Half_edge]): Unit = {
    this.hedges.clear()
    this.hedges ++= hedges.groupBy(_.angleAtOrig)
      .flatMap{ hedge =>
        val h1s = hedge._2
        val h2s = hedge._2
        val h = (h1s.headOption, h2s.headOption) match {
          case (Some(h1), Some(h2)) => { // there are exterior and interior edges...
            h1.twin = h2.twin
            List(h1)
          }
          case (Some(h1), None) => { // there are only interior edges...
            h1s
          }
          case (None, Some(h2)) => { // there are only exterior edges..
            h2s
          }
          case (None, None) => List.empty[Half_edge] // Can't happen!
        }
        h
      }
  }

  def getHedges: List[Half_edge] = {
    this.hedges.toList.sortBy(_.angleAtOrig)(Ordering[Double].reverse)
  }

  def toPoint(implicit geofactory: GeometryFactory): Point = geofactory.createPoint(coord)
}
