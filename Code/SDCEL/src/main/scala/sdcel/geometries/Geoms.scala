package edu.ucr.dblab.sdcel.geometries

import scala.annotation.tailrec
import com.vividsolutions.jts.geom.{GeometryFactory, Coordinate, Geometry}
import com.vividsolutions.jts.geom.{MultiPolygon, Polygon, LineString, LinearRing, Point}
import com.vividsolutions.jts.geomgraph.Edge

case class HEdge(coords: Array[Coordinate], h: Half_edge) extends Edge(coords)

case class EdgeData(polygonId: Int, ringId: Int, edgeId: Int, isHole: Boolean,
  label: String = "A") {
  override def toString: String = s"${label}$polygonId\t$ringId\t$edgeId\t$isHole"
}

case class Tag(label: String, pid: Int){
  override def toString: String = s"$label$pid"
}

case class Half_edge(edge: LineString) {
  private val geofactory = edge.getFactory
  val coords = edge.getCoordinates
  val v1 = coords(0)
  val v2 = coords(1)
  val orig = Vertex(v1)
  val dest = Vertex(v2)
  val data = if(edge.getUserData.isInstanceOf[EdgeData])
    edge.getUserData.asInstanceOf[EdgeData]
  else
    EdgeData(-1,-1,-1,false)
  val wkt = edge.toText()
  var tags: List[Tag] = List.empty[Tag]
  var twin: Half_edge = null
  var next: Half_edge = null
  var prev: Half_edge = null

  def getTag: String = tags.sortBy(_.label).mkString(" ")

  def split(p: Coordinate): List[Half_edge] = {
    if(p == v1 || p == v2 || !edge.getEnvelopeInternal.intersects(p)){
      List(this)
    } else {
      val h0 = this.prev
      val l1 = geofactory.createLineString(Array(v1, p))
      l1.setUserData(data.copy(edgeId = -1))
      val h1 = Half_edge(l1)
      val l2 = geofactory.createLineString(Array(p, v2))
      l2.setUserData(data.copy(edgeId = -1))
      val h2 = Half_edge(l2)
      val h3 = this.next

      try {
        h0.next = h1; h1.next = h2; h2.next = h3;
        h3.prev = h2; h2.prev = h1; h1.prev = h0;
      } catch {
        case e: java.lang.NullPointerException => {
          print(s"Half-edge split error in ${this.wkt} [${this.data}]")
          println(s" at $p")
          println(s"Prev: $h0")
          println(s"Next: $h3")
          System.exit(0)
        }
      }

      List(h1, h2)
    }
  }

  def getPolygon(implicit geofactory: GeometryFactory): Polygon = {

    try {
      val coords = (v1 +: getNexts.map{_.v2}).toArray
      if(coords.size >= 4){
        geofactory.createPolygon(coords)
      } else {
        println("Error creating polygon face. Less than 4 vertices...")
        println(this)
        println("Retriving empty polygon instead...")
        emptyPolygon
      }
    } catch {
      case e: java.lang.IllegalArgumentException => {
        println("Error creating polygon face...")
        println(e.getMessage)
        println(coords.mkString(" "))
        System.exit(0)
        geofactory.createPolygon(coords.toArray)
      }
    }
    
  }

  def getNextsAsWKT(implicit geofactory: GeometryFactory): String = {
    val hedges = getNexts
    val coords = hedges.map{_.v1} :+ hedges.last.v2
    geofactory.createLineString(coords.toArray).toText
  }
  def getPrevsAsWKT(implicit geofactory: GeometryFactory): String = {
    val hedges = getPrevs
    val coords = hedges.map{_.v2} :+ hedges.last.v1
    geofactory.createLineString(coords.toArray).toText
  }

  def getNexts: List[Half_edge] = {
    @tailrec
    def getNextsTailrec(hedges: List[Half_edge], i: Int): List[Half_edge] = {
      val next = hedges.last.next
      if( next == null || next == hedges.head ){
        hedges
      } else {
        getNextsTailrec(hedges :+ next, i+1)
      }
    }
    getNextsTailrec(List(this), 0)
  }
  def getPrevs: List[Half_edge] = {
    @tailrec
    def getPrevsTailrec(hedges: List[Half_edge]): List[Half_edge] = {
      val prev = hedges.last.prev
      if( prev == null || prev == hedges.head){
        hedges
      } else {
        getPrevsTailrec(hedges :+ prev)
      }
    }
    getPrevsTailrec(List(this))
  }

  def updateTags: List[Tag] = {
    @tailrec
    def getNextTailrec(stop: Half_edge, current: Half_edge): List[Tag] = {
      if( current.tags.size == 2){
        current.tags
      } else if(current.label != stop.label){
        current.tags ++ stop.tags
      } else if(current == stop){
        stop.tags
      } else {
        getNextTailrec(stop, current.next)
      }
    }
    getNextTailrec(this, this.next).distinct
  }

  def label: String =
    s"${this.data.label}${if(this.data.polygonId < 0) "" else this.data.polygonId}"

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
    edge.setUserData(this.data.copy(polygonId = -1))
    Half_edge(edge)
  }

  val emptyPolygon: Polygon = {
    val p0 = new Coordinate(0,0)
    geofactory.createPolygon(Array(p0, p0, p0, p0))
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

case class Cell(id: Int, lineage: String, mbr: LinearRing){
  def wkt(implicit geofactory: GeometryFactory) = s"${toPolygon.toText}\t${lineage}\t${id}"

  def toPolygon(implicit geofactory: GeometryFactory): Polygon = {
    geofactory.createPolygon(mbr)
  }
}

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
