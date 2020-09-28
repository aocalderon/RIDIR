package edu.ucr.dblab.sdcel.geometries

import scala.annotation.tailrec
import com.vividsolutions.jts.geom.{GeometryFactory, Coordinate}
import com.vividsolutions.jts.geom.{MultiPolygon, Polygon, LineString, Point}

case class EdgeData(polygonId: Int, ringId: Int, edgeId: Int, isHole: Boolean,
  label: String = "A"){
  override def toString: String = s"${label}$polygonId\t$ringId\t$edgeId\t$isHole"
}

case class Half_edge(edge: LineString) {
  private val coords = edge.getCoordinates
  val v1 = coords(0)
  val v2 = coords(1)
  val orig = Vertex(v1)
  val dest = Vertex(v2)
  val data = edge.getUserData.asInstanceOf[EdgeData]
  val wkt = edge.toText()
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
    edge.setUserData(EdgeData(-1,0,0,false))
    Half_edge(edge)
  }

  def getNexts: List[Half_edge] = {
    @tailrec
    def getNextsTailrec(hedges: List[Half_edge]): List[Half_edge] = {
      val next = hedges.last.next
      println(s"C:${hedges.last} -> ${next}")
      if( next == null || next == hedges.head){
        hedges
      } else {
        getNextsTailrec(hedges :+ next)
      }
    }
    val l = getNextsTailrec(List(this))
    l
  }
}

case class Vertex(coord: Coordinate, hedges: List[Half_edge] = List.empty[Half_edge]) {
  val x = coord.x
  val y = coord.y

  def hedgesNum: Int = hedges.size

  def getIncidentHedges: List[Half_edge] = {
    this.hedges.filter(h => h.dest.x == x && h.dest.y == y)
      .sortBy(_.angleAtDest)(Ordering[Double].reverse)
  }

  def toPoint(implicit geofactory: GeometryFactory): Point = geofactory.createPoint(coord)
}
