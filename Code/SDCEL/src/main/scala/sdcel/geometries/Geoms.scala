package edu.ucr.dblab.sdcel.geometries

import scala.annotation.tailrec
import com.vividsolutions.jts.geom.{GeometryFactory, Coordinate, Geometry}
import com.vividsolutions.jts.geom.{MultiPolygon, Polygon, LineString, LinearRing, Point}
import com.vividsolutions.jts.geomgraph.Edge

case class Tag(label: String, pid: Int){
  override def toString: String = s"$label$pid"
}

case class HEdge(coords: Array[Coordinate], h: Half_edge) extends Edge(coords)
case class LEdge(coords: Array[Coordinate], l: LineString) extends Edge(coords)

case class EdgeData(polygonId: Int, ringId: Int, edgeId: Int, isHole: Boolean,
  label: String = "A", crossingInfo: String = "", nedges: Int = -1) {
  override def toString: String =
    s"${label}$polygonId\t$ringId\t${edgeId}/${nedges}\t$isHole\t$crossingInfo"
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
  var original_coords: Array[Coordinate] = Array.empty[Coordinate]
  var tags: List[Tag] = List.empty[Tag]
  var twin: Half_edge = null
  var next: Half_edge = null
  var prev: Half_edge = null
  var isNewTwin: Boolean = false
  var MAX_RECURSION: Int = Int.MaxValue

  override def toString = s"${edge.toText}\t$data"

  def getPolygonId(): Int = {
    @tailrec
    def polyId(hedge: Half_edge): Int = {
      if(hedge.data.polygonId >= 0){
        hedge.data.polygonId
      } else {
        polyId(hedge.next)
      }
    }

    polyId(this)
  }

  def getTag: String = tags.sortBy(_.label).mkString(" ")

  def split(p: Coordinate): List[Half_edge] = {
    if(p == v1 || p == v2 || !edge.getEnvelopeInternal.intersects(p)){
      List(this) // if intersection is on the extremes or outside (there is not split at all), it returns the same...
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
        println(this)
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

  def getNextsDebug: List[Half_edge] = {
    @tailrec
    def getNextsTailrec(hedges: List[Half_edge], i: Int): List[Half_edge] = {
      val next = hedges.last.next
      //println(next)
      if( next == null || next == hedges.head || i > 100){
        hedges
      } else {
        getNextsTailrec(hedges :+ next, i+1)
      }
    }
    getNextsTailrec(List(this), 0)
  }

  def getNexts: List[Half_edge] = {
    @tailrec
    def getNextsTailrec(hedges: List[Half_edge], i: Int): List[Half_edge] = {

      val next = hedges.last.next
      if( next == null || next == hedges.head){
        hedges
      } else if(i >= MAX_RECURSION){
        List.empty[Half_edge]
      }else {
        getNextsTailrec(hedges :+ next, i + 1)
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
    val h = Half_edge(edge)
    h.isNewTwin = true
    h
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

  override def toString = s"${polygonId}:\n${hedges.map(_.edge)}"

  def wkt(implicit geofactory: GeometryFactory): String = {
    val coords = hedges.map{_.v1} :+ hedges.last.v2
    val s = geofactory.createLineString(coords.toArray)
    s"${s.toText}\t${first.data}"
  }

  def isClose: Boolean = {
    last.v2 == first.v1
  }

  def getExtremes: List[Half_edge] = if(hedges.size == 1) List(first) else List(first, last)

  def tail: Segment = Segment(hedges.tail) 
}

case class Cell(id: Int, lineage: String, mbr: LinearRing){
  val boundary = mbr.getEnvelopeInternal

  def wkt(implicit geofactory: GeometryFactory) = s"${toPolygon.toText}\t${lineage}\t${id}\n"

  def toPolygon(implicit geofactory: GeometryFactory): Polygon = {
    geofactory.createPolygon(mbr)
  }

  def getSouthBorder(implicit geofactory: GeometryFactory): LineString = {
    val se = new Coordinate(boundary.getMinX, boundary.getMinY)
    val sw = new Coordinate(boundary.getMaxX, boundary.getMinY)
    val S = geofactory.createLineString(Array(se, sw))
    S
  }
  def getWestBorder(implicit geofactory: GeometryFactory): LineString = {
    val sw = new Coordinate(boundary.getMaxX, boundary.getMinY)
    val nw = new Coordinate(boundary.getMaxX, boundary.getMaxY)
    val W = geofactory.createLineString(Array(sw, nw))
    W
  }
  def getNorthBorder(implicit geofactory: GeometryFactory): LineString = {
    val nw = new Coordinate(boundary.getMaxX, boundary.getMaxY)
    val ne = new Coordinate(boundary.getMinX, boundary.getMaxY)
    val N = geofactory.createLineString(Array(nw, ne))
    N
  }
  def getEastBorder(implicit geofactory: GeometryFactory): LineString = {
    val se = new Coordinate(boundary.getMinX, boundary.getMinY)
    val ne = new Coordinate(boundary.getMinX, boundary.getMaxY)
    val E = geofactory.createLineString(Array(ne, se))
    E
  }

  def toLEdges(implicit geofactory: GeometryFactory): List[LEdge] = {
    val se = new Coordinate(boundary.getMinX, boundary.getMinY)
    val sw = new Coordinate(boundary.getMaxX, boundary.getMinY)
    val nw = new Coordinate(boundary.getMaxX, boundary.getMaxY)
    val ne = new Coordinate(boundary.getMinX, boundary.getMaxY)
    val S = geofactory.createLineString(Array(se, sw))
    val W = geofactory.createLineString(Array(sw, nw))
    val N = geofactory.createLineString(Array(nw, ne))
    val E = geofactory.createLineString(Array(ne, se))
    S.setUserData("S")
    W.setUserData("W")
    N.setUserData("N")
    E.setUserData("E")
    val lS = LEdge(S.getCoordinates, S)
    val lW = LEdge(W.getCoordinates, W)
    val lN = LEdge(N.getCoordinates, N)
    val lE = LEdge(E.getCoordinates, E)
    List(lS, lW, lN, lE)
  }

  def toHalf_edges(implicit geofactory: GeometryFactory): List[Half_edge] = {
    val se = new Coordinate(boundary.getMinX, boundary.getMinY)
    val sw = new Coordinate(boundary.getMaxX, boundary.getMinY)
    val nw = new Coordinate(boundary.getMaxX, boundary.getMaxY)
    val ne = new Coordinate(boundary.getMinX, boundary.getMaxY)
    val e1 = geofactory.createLineString(Array(se, sw))
    val e2 = geofactory.createLineString(Array(sw, nw))
    val e3 = geofactory.createLineString(Array(nw, ne))
    val e4 = geofactory.createLineString(Array(ne, se))
    e1.setUserData(EdgeData(-1, 0, 1, false, "C"))
    e2.setUserData(EdgeData(-1, 0, 2, false, "C"))
    e3.setUserData(EdgeData(-1, 0, 3, false, "C"))
    e4.setUserData(EdgeData(-1, 0, 4, false, "C"))
    val h1 = Half_edge(e1)
    val h2 = Half_edge(e2)
    val h3 = Half_edge(e3)
    val h4 = Half_edge(e4)
    List(h1,h2,h3,h4)
  }

  def toHalf_edge(polyId: Int, label: String)
    (implicit geofactory: GeometryFactory): Half_edge = {

    val se = new Coordinate(boundary.getMinX, boundary.getMinY)
    val sw = new Coordinate(boundary.getMaxX, boundary.getMinY)
    val nw = new Coordinate(boundary.getMaxX, boundary.getMaxY)
    val ne = new Coordinate(boundary.getMinX, boundary.getMaxY)
    val e1 = geofactory.createLineString(Array(se, sw))
    val e2 = geofactory.createLineString(Array(sw, nw))
    val e3 = geofactory.createLineString(Array(nw, ne))
    val e4 = geofactory.createLineString(Array(ne, se))
    e1.setUserData(EdgeData(polyId, 0, 1, false, label))
    e2.setUserData(EdgeData(polyId, 0, 2, false, label))
    e3.setUserData(EdgeData(polyId, 0, 3, false, label))
    e4.setUserData(EdgeData(polyId, 0, 4, false, label))
    val h1 = Half_edge(e1)
    val h2 = Half_edge(e2)
    val h3 = Half_edge(e3)
    val h4 = Half_edge(e4)
    h1.next = h2; h1.prev = h4
    h2.next = h3; h2.prev = h1
    h3.next = h4; h3.prev = h2
    h4.next = h1; h4.prev = h3
    h1
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
