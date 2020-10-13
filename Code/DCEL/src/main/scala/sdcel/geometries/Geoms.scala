package edu.ucr.dblab.sdcel.geometries

import scala.annotation.tailrec
import com.vividsolutions.jts.geom.{GeometryFactory, Coordinate}
import com.vividsolutions.jts.geom.{MultiPolygon, Polygon, LineString, LinearRing, Point}

case class EdgeData(polygonId: Int, ringId: Int, edgeId: Int, isHole: Boolean,
  label: String = "A") {
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

  def getPolygon(implicit geofactory: GeometryFactory): Polygon = {
    val coords = v1 +: getNexts.map{_.v2}
    geofactory.createPolygon(coords.toArray)
  }

  def getNextsAsWKT(implicit geofactory: GeometryFactory): String = {
    val hedges = getNexts
    val coords = hedges.map{_.v1} :+ hedges.last.v2
    geofactory.createLineString(coords.toArray).toText
  }

  private def getNexts: List[Half_edge] = {
    @tailrec
    def getNextsTailrec(hedges: List[Half_edge]): List[Half_edge] = {
      val next = hedges.last.next
      if( next == null || next == hedges.head){
        hedges
      } else {
        getNextsTailrec(hedges :+ next)
      }
    }
    getNextsTailrec(List(this))
  }

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

case class Segment(hedges: List[Half_edge]) {
  val first = hedges.head
  val last = hedges.last
  val polygonId = first.data.polygonId
  val startId = first.data.edgeId
  val endId = last.data.edgeId

  override def toString = s"${polygonId}:${startId}:${endId} "

  def wkt(implicit geofactory: GeometryFactory): String = {
    val coords = hedges.map{_.v1} :+ hedges.last.v2
    geofactory.createLineString(coords.toArray).toText
  }
}

case class Cell(id: Int, lineage: String, mbr: LinearRing)
