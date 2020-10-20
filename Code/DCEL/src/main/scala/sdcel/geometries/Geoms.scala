package edu.ucr.dblab.sdcel.geometries

import scala.annotation.tailrec
import com.vividsolutions.jts.geom.{GeometryFactory, Coordinate, Geometry}
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

case class Face(outer: Half_edge) {
  val polygonId = outer.data.polygonId
  val ringId = outer.data.ringId
  val isHole = outer.data.isHole
  var inners: Vector[Face] = Vector.empty[Face]

  /***
   * Compute area of irregular polygon
   * More info at https://www.mathopenref.com/coordpolygonarea2.html
   ***/
  def outerArea: Double = {
    var area: Double = 0.0
    var h = outer
    do{
      area += (h.v1.x + h.v2.x) * (h.v1.y - h.v2.y)
      h = h.next
    }while(h != outer)

    area / -2.0
  }

  private def getCoordinates: Array[Coordinate] = {
    @tailrec
    def getCoords(current: Half_edge, end: Half_edge, v: Array[Coordinate]):
        Array[Coordinate] = {
      if(current == end) {
        v
      } else {
        getCoords(current.next, end, v :+ current.v2)
      }
    }
    // Adding the first two vertices so it will be start on next...
    getCoords(outer.next, outer, Array(outer.v1, outer.v2))
  }

  def toPolygon(implicit geofactory: GeometryFactory): Polygon = {
    // Need to add first vertex at the end to close the polygon...
    val coords = getCoordinates :+ outer.v1
    geofactory.createPolygon(coords)
  }

  def getPolygons(implicit geofactory: GeometryFactory): Vector[Polygon] = {
    toPolygon +: inners.map(_.toPolygon)
  }

  def getGeometry(implicit geofactory: GeometryFactory): Geometry = {
    val polys = getPolygons
    val geom = if(polys.size == 1){
      polys.head // Return a polygon...
    } else {
      // Groups polygons if one contains another...
      val pairs = for{
        outer <- polys
        inner <- polys
      } yield (outer, inner)

      val partial = pairs.filter{ case (outer, inner) =>
        try{
          inner.coveredBy(outer)
        } catch {
          case e: com.vividsolutions.jts.geom.TopologyException => {
            false
          }
        }
      }.groupBy(_._1).mapValues(_.map(_._2))
      // Set of inner polygons...
      val diff = partial.values.flatMap(_.tail).toSet
      val result = partial.filterKeys(partial.keySet.diff(diff)).values

      val polygons = result.map{ row =>
        val outerPoly = row.head
        val innerPolys = geofactory.createMultiPolygon(row.tail.toArray)
        try{
          outerPoly.difference(innerPolys).asInstanceOf[Polygon]
        } catch {
          case e: com.vividsolutions.jts.geom.TopologyException => {
            geofactory.createPolygon(Array.empty[Coordinate])
          }
        }        
      }

      polygons.size match {
        // Return an empty polygon...
        case 0 => geofactory.createPolygon(Array.empty[Coordinate])
        // Return a polygon with holes...
        case 1 => polygons.head
        // Return a multi-polygon...
        case x if x > 1 => geofactory.createMultiPolygon(polygons.toArray)
      }
    }
    geom match {
      case geom if geom.isInstanceOf[Polygon] =>{
        val poly = geom.asInstanceOf[Polygon]
        if(poly.getNumInteriorRing > 0){
          geom
        } else {
          if(poly.isEmpty()){
            geom
          } else {
            geom
          }
        }
      }
      case geom if geom.isInstanceOf[MultiPolygon] => geom
    }
  }

  def toWKT(implicit geofactory: GeometryFactory): String = getGeometry.toText
}
