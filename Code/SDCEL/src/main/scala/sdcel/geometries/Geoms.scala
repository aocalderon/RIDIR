package edu.ucr.dblab.sdcel.geometries

import scala.annotation.tailrec
import com.vividsolutions.jts.geom.{GeometryFactory, Coordinate, Geometry, Envelope}
import com.vividsolutions.jts.geom.{MultiPolygon, Polygon, LineString, LinearRing, Point}
import com.vividsolutions.jts.geomgraph.Edge

import edu.ucr.dblab.sdcel.Utils.logger

case class Tag(label: String, pid: Int){
  override def toString: String = s"$label$pid"
}

case class HEdge(coords: Array[Coordinate], h: Half_edge) extends Edge(coords)
case class LEdge(coords: Array[Coordinate], l: LineString) extends Edge(coords)

case class EdgeData(polygonId: Int, ringId: Int, edgeId: Int, isHole: Boolean,
  label: String = "A", crossingInfo: String = "None", nedges: Int = -1) {

  private var cellBorder = false
  def setCellBorder(cb: Boolean) = { cellBorder = cb }
  def getCellBorder(): Boolean = { cellBorder }

  private var wkt: String = ""
  def setWKT(w: String) = { wkt = w }
  def getWKT: String = { wkt }

  private var invalidVertex: Int = 0
  def setInvalidVertex(iv: Int) = { invalidVertex = iv }
  def getInvalidVertex: Int = { invalidVertex }

  override def toString: String =
    s"${label}$polygonId\t$ringId\t${edgeId}/${nedges}\t$isHole\t$crossingInfo"
}

case class Half_edge(edge: LineString) {
  private val geofactory = edge.getFactory
  val coords = edge.getCoordinates
  val v1 = coords(0)
  val v2 = coords(1)
  val data = if(edge.getUserData.isInstanceOf[EdgeData])
    edge.getUserData.asInstanceOf[EdgeData]
  else
    EdgeData(-1,-1,-1,false)
  //val orig = Vertex(v1)
  //val dest = Vertex(v2)

  var (orig, dest) = data.getInvalidVertex match {
    case 0 => (Vertex(v1), Vertex(v2))
    case 1 => (Vertex(v1, false), Vertex(v2))
    case 2 => (Vertex(v1), Vertex(v2, false))
    case 3 => (Vertex(v1, false), Vertex(v2, false))
  }

  val wkt = edge.toText()
  var original_coords: Array[Coordinate] = Array.empty[Coordinate]
  var tags: List[Tag] = List.empty[Tag]
  var twin: Half_edge = null
  var next: Half_edge = null
  var prev: Half_edge = null
  var isNewTwin: Boolean = false
  var mbr: Envelope = null
  var poly: Polygon = null
  var tag: String = null
  var source: Coordinate = v1
  var target: Coordinate = v2
  var MAX_RECURSION: Int = Int.MaxValue

  override def toString = s"${edge.toText}\t$data\t${orig.valid} ${dest.valid}"

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

  def getLabelId(): String = {
    @tailrec
    def polyId(hedge: Half_edge): String = {
      if(hedge.data.polygonId >= 0){
        val pid = hedge.data.polygonId
        val lab = hedge.data.label
        s"$lab$pid"
      } else {
        polyId(hedge.next)
      }
    }

    polyId(this)
  }

  def hasCrossingInfo: Boolean = data.crossingInfo != "None"

  def getCrossingCoord: Option[Coordinate] = {
    if(hasCrossingInfo){
      val info = data.crossingInfo
      val cross = info.split("\\|").head
      val arr = cross.split(":")
      val xy = arr(1).split(" ")
      val coord = new Coordinate(xy(0).toDouble, xy(1).toDouble)
      Some(coord)
    } else {
      None
    }
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

  def checkValidity(implicit geofactory: GeometryFactory): Boolean = {
    try {
      val coords = (v1 +: getNexts.map{_.v2}).toArray
      if(coords.size >= 4){
        geofactory.createPolygon(coords)
        true
      } else {
        false
      }
    } catch {
      case e: java.lang.IllegalArgumentException => {
        false
      }
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
        emptyPolygon
      }
    } catch {
      case e: java.lang.IllegalArgumentException => {
        println("Error creating polygon face...")
        //println(e.getMessage)
        //println(coords.mkString(" "))
        println(this)
        emptyPolygon
      }
    }
  }

  def isClose(): Boolean = {
    val nexts = this.getNexts
    this.v1 == nexts.last.v2
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

  def getMinx: Double = { if(this.v1.x < this.v2.x) this.v1.x else this.v2.x }
  def getMiny: Double = { if(this.v1.y < this.v2.y) this.v1.y else this.v2.y }
  def getMaxx: Double = { if(this.v1.x > this.v2.x) this.v1.x else this.v2.x }
  def getMaxy: Double = { if(this.v1.y > this.v2.y) this.v1.y else this.v2.y }

  def getNextsMBR: (List[Half_edge],Envelope, String) = {
    @tailrec
    def getNextsTailrec(hedges: List[Half_edge],
      minx:Double,miny:Double,maxx:Double,maxy:Double,tags:Set[String])
        : (List[Half_edge], Envelope, String) = {
      val next = hedges.last.next
      if( next == null || next == hedges.head){
        val mbr = new Envelope(minx,maxx,miny,maxy)
        hedges.head.mbr = mbr
        (hedges, mbr, tags.toList.sorted.mkString(" "))
      } else {
        val x1 = next.getMinx
        val minx1 = if(x1 < minx) x1 else minx 
        val y1 = next.getMiny
        val miny1 = if(y1 < miny) y1 else miny 
        val x2 = next.getMaxx
        val maxx2 = if(x2 > maxx) x2 else maxx 
        val y2 = next.getMaxy
        val maxy2 = if(y2 > maxy) y2 else maxy
        val ntag = if(next.tag != null){
          tags ++ Set(next.tag)
        } else tags
        getNextsTailrec(hedges :+ next, minx1, miny1, maxx2, maxy2, ntag)
      }
    }
    val minx = Double.MaxValue
    val miny = Double.MaxValue
    val maxx = Double.MinValue
    val maxy = Double.MinValue
    val tags = Set(this.tag)
    getNextsTailrec(List(this),minx,miny,maxx,maxy,tags)
  }

  def getNextsMBRPoly(implicit geofactory: GeometryFactory)
      : (List[Half_edge],Envelope, Polygon) = {
    @tailrec
    def getNextsTailrec(hedges: List[Half_edge], coords: Array[Coordinate],
      minx:Double,miny:Double,maxx:Double,maxy:Double, tag: String)
      (implicit geofactory: GeometryFactory): (List[Half_edge], Envelope, Polygon) = {

      val next = hedges.last.next
      if( next == null || next == hedges.head){
        val mbr = new Envelope(minx,maxx,miny,maxy)
        hedges.head.mbr = mbr
        val ring = coords :+ hedges.head.v1
        val poly = if(ring.size >= 4)
          geofactory.createPolygon(ring)
        else{
          logger.info(s"${hedges.head.toString}")
          hedges.foreach(println)
          geofactory.createPolygon(Array.empty[Coordinate])
        }
        hedges.head.poly = poly
        //val hs = hedges.map{ h => h.tag = tag; h}
        (hedges, mbr, poly)
      } else {
        val x1 = next.getMinx
        val minx1 = if(x1 < minx) x1 else minx 
        val y1 = next.getMiny
        val miny1 = if(y1 < miny) y1 else miny 
        val x2 = next.getMaxx
        val maxx2 = if(x2 > maxx) x2 else maxx 
        val y2 = next.getMaxy
        val maxy2 = if(y2 > maxy) y2 else maxy
        val coord = next.v2
        next.tag = tag
        getNextsTailrec(hedges :+ next, coords :+ coord, minx1, miny1, maxx2, maxy2, tag)
      }
    }
    val minx = Double.MaxValue
    val miny = Double.MaxValue
    val maxx = Double.MinValue
    val maxy = Double.MinValue
    val coords = Array(this.v1, this.v2)
    val tag = this.getLabelId
    this.tag = tag

    getNextsTailrec(List(this), coords, minx, miny, maxx, maxy, tag)
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

  //TODO
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
    // I need mark if the twin is for a border edge...
    val code = if(this.data.polygonId == -1) -1 else -2
    val new_data = this.data.copy(polygonId = code)
    edge.setUserData(new_data)
    val h = Half_edge(edge)
    h.isNewTwin = true
    h.orig = this.dest
    h.dest = this.orig
    h
  }

  def isValid: Boolean = {!isNotValid}
  
  def isNotValid: Boolean = {
    data.polygonId == -1 || data.getCellBorder == true
  }

  val emptyPolygon: Polygon = {
    val p0 = new Coordinate(0,0)
    geofactory.createPolygon(Array(p0, p0, p0, p0))
  }
}

case class Vertex(coord: Coordinate, valid: Boolean = true) {
  val x = coord.x
  val y = coord.y

  def toPoint(implicit geofactory: GeometryFactory): Point = geofactory.createPoint(coord)
}

case class Coord(coord: Coordinate, valid: Boolean){

  override def toString: String = s"(${coord.x} ${coord.y} $valid)"
}

case class Coords(coords: Array[Coord]){
  val size = coords.size
  val first = coords.head
  val last  = coords.last

  override def toString: String = coords.toList.toString

  def getCoords: Array[Coordinate] = {
    /*
    if(first == last){
      coords.map(_.coord)
    } else {
      val c = coords.filter(_.valid).map(_.coord)
      c :+ c.head
    }
     */

    val C = coords.filter(_.valid).map(_.coord)
    if(C.isEmpty){
      coords.map(_.coord)
    } else {
      val new_coords = if(C.head == C.last)
        C
      else
        if(C.isEmpty) C else C :+ C.head
      new_coords
    }
  }

  def touch(c: Coordinate, tolerance: Double = 1e-2): Boolean = {
    if(c == last){
      true
    } else {
      val rx = last.coord.x
      val ry = last.coord.y
      val bool = rx - tolerance < c.x && c.x < rx + tolerance &&
      ry - tolerance < c.y && c.y < ry + tolerance
      bool
    }
  }

  def toWKT(implicit geofactory: GeometryFactory): String = {
    val wkt = geofactory.createLineString(coords.map(_.coord))

    s"$wkt"
  }

  def toWKT2(implicit geofactory: GeometryFactory): String = {
    val C = coords.filter(_.valid).map(_.coord)
    val new_coords = if(C.head == C.last) C else C :+ C.head
    val wkt = geofactory.createPolygon(new_coords)

    s"$wkt"
  }

  def isClose: Boolean = {
    this.touch(first.coord)
  }
}

case class Segment(hedges: List[Half_edge]) {
  val first = hedges.head
  val last = hedges.last
  val polygonId = first.data.polygonId
  val ringId = first.data.ringId
  val startId = first.data.edgeId
  val endId = last.data.edgeId

  override def toString = s"${polygonId}:\n${hedges.map(_.edge)}"

  def wkt(implicit geofactory: GeometryFactory): String = {
    val coords = hedges.map{_.v1} :+ hedges.last.v2
    val s = geofactory.createLineString(coords.toArray)
    s"${s.toText}\t${first.data}"
  }

  def getLine(implicit geofactory: GeometryFactory): String = {
    if(isClose){
      val coords = hedges.map{_.v1} :+ hedges.last.v2
      val s = geofactory.createLineString(coords.toArray)
      s"${s.toText}\t${isHole}\tN"      
    } else {
      val extremeToRemove = 
        if(first.orig.valid && last.dest.valid){
          "N"
        } else if(!first.orig.valid && last.dest.valid){
          "S"
        } else if(first.orig.valid && !last.dest.valid){
          "E"
        } else {
          "B"
        }

      val coords = hedges.map{_.v1} :+ hedges.last.v2
      val s = geofactory.createLineString(coords.toArray)
      s"${s.toText}\t${isHole}\t${extremeToRemove}"
    }
  }

  def line(implicit geofactory: GeometryFactory): String = {
    if(isClose){
      val coords = hedges.map{_.v1} :+ hedges.last.v2
      val s = geofactory.createLineString(coords.toArray)
      s"${s.toText}\t${isHole}\tN"      
    } else {
      val extremeToRemove = if(hedges.size > 1){
        if(first.data.crossingInfo != "None" &&
          last.data.crossingInfo != "None"){
          "B"
        } else if(first.data.crossingInfo != "None"){
          "S"
        } else if(last.data.crossingInfo != "None"){
          "E"
        } else {
          "X"
        }
      } else {
        val cinfo = first.data.crossingInfo
        if(cinfo == "None"){
          "X"
        } else {
          val arr = cinfo.split("\\|")
          if(arr.size > 1){
            "B"
          } else {
            val arr2 = arr.head.split(":")
            val xy = arr2(1).split(" ")
            val C = new Coordinate(xy(0).toDouble, xy(1).toDouble)
            if(C.distance(first.v1) < C.distance(first.v2)){
              "S"
            } else {
              "E"
            }
          }
        }
      }

      val coords = hedges.map{_.v1} :+ hedges.last.v2
      val s = geofactory.createLineString(coords.toArray)
      s"${s.toText}\t${isHole}\t${extremeToRemove}"
    }
  }

  def isHole: Int = if(hedges.exists(!_.data.isHole)) 0 else 1

  def isClose: Boolean = {
    last.v2 == first.v1
  }

  def getExtremes: List[Half_edge] = if(hedges.size == 1) List(first) else List(first, last)

  def tail: Segment = Segment(hedges.tail) 
}

case class Cell(id: Int, lineage: String, mbr: LinearRing){
  val boundary = mbr.getEnvelopeInternal

  def wkt(implicit geofactory: GeometryFactory) = s"${toPolygon.toText}\t${lineage}\t${id}"

  def toPolygon(implicit geofactory: GeometryFactory): Polygon = {
    geofactory.createPolygon(mbr)
  }

  def getSouthBorder(implicit geofactory: GeometryFactory): LineString = {
    val sw = new Coordinate(boundary.getMinX, boundary.getMinY)
    val se = new Coordinate(boundary.getMaxX, boundary.getMinY)
    val S = geofactory.createLineString(Array(sw, se))
    S
  }
  def getEastBorder(implicit geofactory: GeometryFactory): LineString = {
    val se = new Coordinate(boundary.getMaxX, boundary.getMinY)
    val ne = new Coordinate(boundary.getMaxX, boundary.getMaxY)
    val E = geofactory.createLineString(Array(se, ne))
    E
  }
  def getNorthBorder(implicit geofactory: GeometryFactory): LineString = {
    val ne = new Coordinate(boundary.getMaxX, boundary.getMaxY)
    val nw = new Coordinate(boundary.getMinX, boundary.getMaxY)
    val N = geofactory.createLineString(Array(ne, nw))
    N
  }
  def getWestBorder(implicit geofactory: GeometryFactory): LineString = {
    val nw = new Coordinate(boundary.getMinX, boundary.getMaxY)
    val sw = new Coordinate(boundary.getMinX, boundary.getMinY)
    val W = geofactory.createLineString(Array(nw, sw))
    W
  }

  def toLEdges(implicit geofactory: GeometryFactory): List[LEdge] = {
    val sw = new Coordinate(boundary.getMinX, boundary.getMinY)
    val se = new Coordinate(boundary.getMaxX, boundary.getMinY)
    val ne = new Coordinate(boundary.getMaxX, boundary.getMaxY)
    val nw = new Coordinate(boundary.getMinX, boundary.getMaxY)
    val S = geofactory.createLineString(Array(sw, se))
    val E = geofactory.createLineString(Array(se, ne))
    val N = geofactory.createLineString(Array(ne, nw))
    val W = geofactory.createLineString(Array(nw, sw))
    S.setUserData("S")
    E.setUserData("E")
    N.setUserData("N")
    W.setUserData("W")
    val lS = LEdge(S.getCoordinates, S)
    val lE = LEdge(E.getCoordinates, E)
    val lN = LEdge(N.getCoordinates, N)
    val lW = LEdge(W.getCoordinates, W)
    List(lS, lE, lN, lW)
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
    val ed1 = EdgeData(polyId, -1, 1, false, label)
    ed1.setCellBorder(true)
    val ed2 = EdgeData(polyId, -1, 2, false, label)
    ed2.setCellBorder(true)
    val ed3 = EdgeData(polyId, -1, 3, false, label)
    ed3.setCellBorder(true)
    val ed4 = EdgeData(polyId, -1, 4, false, label)
    ed4.setCellBorder(true)
    e1.setUserData(ed1)
    e2.setUserData(ed2)
    e3.setUserData(ed3)
    e4.setUserData(ed4)
    val tag = s"$label$polyId"
    val h1 = Half_edge(e1)
    h1.tag = tag
    val h2 = Half_edge(e2)
    h2.tag = tag
    val h3 = Half_edge(e3)
    h3.tag = tag
    val h4 = Half_edge(e4)
    h4.tag = tag
    h1.next = h2; h1.prev = h4
    h2.next = h3; h2.prev = h1
    h3.next = h4; h3.prev = h2
    h4.next = h1; h4.prev = h3
    h1
  }
}

case class Face(outer: Half_edge, label: String = "") {
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
