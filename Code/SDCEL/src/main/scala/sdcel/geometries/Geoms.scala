package edu.ucr.dblab.sdcel.geometries

import org.apache.spark.TaskContext

import com.vividsolutions.jts.geom.{GeometryFactory, Coordinate, Geometry, Envelope}
import com.vividsolutions.jts.geom.{MultiPolygon, Polygon, LineString, LinearRing, Point}
import com.vividsolutions.jts.geomgraph.Edge
import com.vividsolutions.jts.io.WKTReader

import com.google.common.collect.TreeMultiset

import scala.collection.JavaConverters._
import scala.annotation.tailrec
import java.util.Comparator

import edu.ucr.dblab.sdcel.Utils.logger
import edu.ucr.dblab.debug.AVLTreeST

case class Tag(label: String, pid: Int){
  override def toString: String = s"$label$pid"
}

case class HEdge(coords: Array[Coordinate], h: Half_edge)  extends Edge(coords)
case class LEdge(coords: Array[Coordinate], l: LineString) extends Edge(coords)

import edu.ucr.dblab.bo.{Point => BOPoint, Segment => BOSegment}
case class Seg(hedge: Half_edge) {
  val edge: BOSegment = {
    val p1 = new BOPoint(hedge.left.x, hedge.left.y)
    val p2 = new BOPoint(hedge.right.x, hedge.right.y)
    new BOSegment(p1, p2)
  }
}

object EmptyCoordinate {
  private val coordinate: Coordinate  = new Coordinate()
  coordinate.setOrdinate(0, Double.NaN)
  coordinate.setOrdinate(1, Double.NaN)
  def getCoordinate: Coordinate = coordinate
}

sealed trait Label
case object A extends Label
case object B extends Label


//////////////////////////////////////////////////////////////////////////////////////////////////////
sealed trait Event
case object LEFT_ENDPOINT  extends Event
case object RIGHT_ENDPOINT extends Event
case object INTERSECTION   extends Event

object StatusKey{
  def intersection(one: Option[StatusValue], another: Option[StatusValue], p: Coordinate)
      (implicit geofactory: GeometryFactory): Boolean = {
    one match{
      case None => false
      case Some(one) => {
        another match {
          case None => false
          case Some(another) => one.hedge.intersection(another.hedge).exists( _.x > p.x )
        }
      }
    }
  }

  def above(value: StatusValue)(implicit status: AVLTreeST[StatusKey, StatusValue],
    geofactory: GeometryFactory): Option[StatusValue] = {

    val values = status.values().asScala.toList
    values.find(_.hedge.tag == value.hedge.tag) match {
      case Some(s) => Some( values(values.indexOf(s) + 1) )
      case None => None
    }
  }

  def below(value: StatusValue)(implicit status: AVLTreeST[StatusKey, StatusValue],
    geofactory: GeometryFactory): Option[StatusValue] = {

    val values = status.values().asScala.toList
    values.find(_.hedge.tag == value.hedge.tag) match {
      case Some(s) => Some( values(values.indexOf(s) - 1) )
      case None => None
    }
  }

  def above(s1: StatusKey, s2: StatusKey): Boolean = {
    isAbove(s2.left, s1) match {
      case  1 => true
      case -1 => false
      case  0 => {
        isAbove(s2.right, s1) match {
          case  1 => true
          case -1 => false
          case  0 => false // TODO: lines are colinear
        }
      }
    }
  }

  def above(s: StatusKey, p1: Coordinate, p2: Coordinate): Boolean = {
    isAbove(p1, s) match {
      case  1 => true
      case -1 => false
      case  0 => {
        isAbove(p2, s) match {
          case  1 => true
          case -1 => false
          case  0 => false // TODO: lines are colinear
        }
      }
    }
  }

  def isAbove(T: Coordinate, s: StatusKey): Int = {
    val P = s.left
    val Q = s.right
    val a = (Q.y - P.y) / (Q.x - P.x)
    val b = 1
    val c = P.y - (a * P.x)

    val f =  a * T.x - b * T.y + c

    f match {
      case x if x <  0 =>  1 
      case x if x == 0 =>  0 // Point T is colinear... 
      case x if x >  0 => -1 
    }
  }
}
case class Sweep(x: Double)
case class StatusValue(key: StatusKey, hedge: Half_edge)
case class StatusKey(left: Coordinate, right: Coordinate, tag: String = "*")
  (implicit geofactory: GeometryFactory) extends Ordered[StatusKey]{
  var p: Coordinate = left
  val maxY = List(left, right).maxBy(_.y).y
  val minY = List(left, right).minBy(_.y).y

  val hedge: Half_edge = {
    val l = geofactory.createLineString(Array(left, right))
    val h = Half_edge(l)
    h.tag = tag
    h
  }

  private def getSweepline: LineString = {
    geofactory.createLineString(Array(new Coordinate(p.x, minY), new Coordinate(p.x, maxY)))
  }

  private def sweeplineIntersection: Coordinate = this.getSweepline.intersection(this.getLine)
    .getCoordinates.minBy(_.y)

  private def getLine: LineString = hedge.edge

  // If point c is left to line a-b [> 0: Left, < 0: Right, = 0: Colinear]...
  //def locate(point: Coordinate): Double =
  //  (right.x - left.x) * (point.y - left.y) - (right.y - left.y) * (point.x - left.x)

  //def compare(that: StatusKey): Int = {
  //  StatusKey.isAbove(this.p, that)
  //}

  //def compare(that: StatusKey): Int = {
  //  that.locate(this.sweep) match {
  //    case x if x  > 0 =>  1
  //    case x if x == 0 =>  0 // Point T is colinear...
  //    case x if x <  0 => -1
  //  }
  //}

  def compare(that: StatusKey): Int = {
    p = this.sweeplineIntersection 
    p.y compare that.sweeplineIntersection.y
  }

  def above(status: AVLTreeST[StatusKey, StatusValue]): Option[StatusValue] = {
    val rank = status.rank(this)
    try {
      Some( status.get(status.select(rank + 1)) )
    } catch {
      case e: Exception => None
    }
  }

  def below(status: AVLTreeST[StatusKey, StatusValue]): Option[StatusValue] = {
    val rank = status.rank(this)
    try {
      Some( status.get(status.select(rank - 1)) )
    } catch {
      case e: Exception => None
    }
  }

  def intersects(that: Option[StatusValue]): Boolean = {
    try{
      that match {
        case Some(that) => {
          hedge.intersects(that.hedge)
        }
        case None => false
      }
    } catch {
      case e: java.lang.NullPointerException => {
        println(this)
        println(that)
        System.exit(0)
        false
      }
    }
  }

  def intersection(that: StatusValue): Coordinate     = this.hedge.intersection(that.hedge).head
  def intersection(sweepline: LineString): Coordinate = this.getLine.intersection(sweepline)
    .getCoordinates.minBy(_.y)

  override def toString: String = this.hedge.tag 
}

case class EventPoint(hedges: List[Half_edge], event: Event, id: Int = -1,
  intersection: Coordinate = EmptyCoordinate.getCoordinate) {

  val head: Half_edge = hedges.head
  val last: Half_edge = hedges.last

  def getEventPoint = {
    val h = hedges.head
    val endpoints = h.endpoints
    event match {
      case LEFT_ENDPOINT  => if(!h.isVertical) endpoints.minBy(_.x) else endpoints.minBy(_.y)
      case RIGHT_ENDPOINT => if(!h.isVertical) endpoints.maxBy(_.x) else endpoints.maxBy(_.y)
      case INTERSECTION   => intersection
    }
  }

  def getLeftEventPoint = {
    val h = hedges.head
    val endpoints = h.endpoints
    if(!h.isVertical) endpoints.minBy(_.x) else endpoints.minBy(_.y)
  }
}
object CoordYX_Ordering extends Ordering[Coordinate] {
  def compare(a:Coordinate, b: Coordinate) = {
    if( a.y != b.y) a.y compare b.y else a.x compare b.x
  }
}
object EventPoint_Ordering extends Ordering[EventPoint] {
  def compare(a: EventPoint, b: EventPoint) = {
    val ap = a.getEventPoint
    val bp = b.getEventPoint
    if( bp.x != ap.x ){
      bp.x compare ap.x
    } else if( bp.y != ap.y ){
      bp.y compare ap.y
    } else {
      0
    }
  }
}
//////////////////////////////////////////////////////////////////////////////////////////////////////

case class EdgeData(polygonId: Int, ringId: Int, edgeId: Int, isHole: Boolean,
  label: String = "A", crossingInfo: String = "None", nedges: Int = -1) {

  def isCrossingBorder: Boolean = crossingInfo != "None"

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

  def getParams: String = s"$label\t$polygonId\t$ringId\t$edgeId\t$isHole"

  def save: String = s"${polygonId}_${ringId}_${edgeId}_${isHole}_${label}_${cellBorder}"
}

object Half_edge {
  
  def save(hedge: Half_edge, pid: Int): String = {
    val  id  = hedge.id
    val wkt  = hedge.edge.toText
    val data = hedge.data.save
    val prev = try{ hedge.prev.id } catch { case e: Throwable => -1L}
    val twin = try{ hedge.twin.id } catch { case e: Throwable => -1L}
    val next = try{ hedge.next.id } catch { case e: Throwable => -1L}

    s"$wkt\t$pid\t$id\t$prev\t$twin\t$next\t$data"
  }

  def load(line: String)(implicit geofactory: GeometryFactory): Half_edge = {
    val arr  = line.split("\t")
    val wkt  = arr(0)
    val pid  = arr(1).toInt
    val curr_id = arr(2).toLong
    val prev_id = try{ arr(3).toLong } catch { case e: Throwable => -1L }
    val twin_id = try{ arr(4).toLong } catch { case e: Throwable => -1L }
    val next_id = try{ arr(5).toLong } catch { case e: Throwable => -1L }

    val data_arr = arr(6).split("_")
    val polyId = data_arr(0).toInt
    val ringId = data_arr(1).toInt
    val edgeId = data_arr(2).toInt
    val hole   = data_arr(3).toBoolean
    val label  = data_arr(4)
    val cellb  = data_arr(5).toBoolean
    val data   = EdgeData(polyId, ringId, edgeId, hole, label)
    data.setCellBorder(cellb)

    val reader = new WKTReader(geofactory)
    val edge = reader.read(wkt).asInstanceOf[LineString]
    edge.setUserData(data)

    val h = Half_edge(edge)
    h.id = curr_id
    h.pointers = List(prev_id, twin_id, next_id)
    h.partitionId = pid
    h
  }
}

case class Half_edge(edge: LineString){
  val coords = edge.getCoordinates
  val v1 = coords(0)
  val v2 = coords(1)
  val data = if(edge.getUserData.isInstanceOf[EdgeData])
    edge.getUserData.asInstanceOf[EdgeData]
  else
    EdgeData(-1,-1,-1,false)
  val params = data.getParams

  var (orig, dest) = data.getInvalidVertex match {
    case 0 => (Vertex(v1), Vertex(v2))
    case 1 => (Vertex(v1, false), Vertex(v2))
    case 2 => (Vertex(v1), Vertex(v2, false))
    case 3 => (Vertex(v1, false), Vertex(v2, false))
  }

  val endpoints = List(v1, v2)
  val left  = endpoints.minBy(_.x)
  val right = endpoints.maxBy(_.x)
  val above = endpoints.maxBy(_.y)
  val below = endpoints.minBy(_.y)
  val minX = left.x
  val minY = below.y

  val wkt = edge.toText()
  var id: Long = 0L
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
  var pointers: List[Long] = List(-1L, -1L, -1L)
  var partitionId: Int = -1
  var MAX_RECURSION: Int = Int.MaxValue

  override def toString = s"${edge.toText}\t$data\t${tag}"

  def getLabelEnum: Label = if(data.label == "A") A else B

  def intersects(that: Half_edge): Boolean = this.edge.intersects(that.edge)
  def intersection(that: Half_edge): Array[Coordinate] = {
    this.edge.intersection(that.edge).getCoordinates
  }


  def isVertical: Boolean   = v1.x == v2.x
  def isHorizontal: Boolean = v1.y == v2.y

  def leftPlusDelta(delta: Double):  Coordinate = new Coordinate(left.x, left.y + delta)
  def rightPlusDelta(delta: Double): Coordinate = new Coordinate(right.x, right.y + delta)

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

  def split(p: Coordinate)(implicit geofactory: GeometryFactory): List[Half_edge] = {
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
        logger.info("Error creating polygon face. Less than 4 vertices...")
        logger.info(this.toString)
        emptyPolygon
      }
    } catch {
      case e: java.lang.IllegalArgumentException => {
        logger.info("Error creating polygon face...")
        //println(e.getMessage)
        //println(coords.mkString(" "))
        logger.info(this.toString)
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
      if( next == null || next == hedges.head || hedges.exists(_ == next)){
        val mbr = new Envelope(minx,maxx,miny,maxy)
        hedges.head.mbr = mbr
        val ftag = if(tags.exists(_.split(" ").size > 1)){
          tags.filter(_.split(" ").size > 1).head
        } else {
          tags.toList.sorted.mkString(" ")
        }
        (hedges, mbr, ftag)
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
      if( next == null || next == hedges.head || hedges.exists(_ == next)){
        val mbr = new Envelope(minx,maxx,miny,maxy)
        hedges.head.mbr = mbr
        val ring = coords :+ hedges.head.v1
        val poly = if(ring.size >= 4)
          geofactory.createPolygon(ring)
        else{
          logger.info(s"${hedges.head.toString}")
          logger.info(hedges.mkString("\n"))
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
      if( next == null || next == hedges.head || hedges.exists(_ == next)){
        hedges
      } else if(i >= MAX_RECURSION){
        hedges :+ hedges.head
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

  def reverseHole(implicit geofactory: GeometryFactory): Half_edge = {
    val edge = geofactory.createLineString(Array(v2, v1))
    edge.setUserData(this.data)
    val h = Half_edge(edge)
    h.orig = this.dest
    h.dest = this.orig
    h
  }

  def isValid: Boolean = {!isNotValid}
  
  def isNotValid: Boolean = {
    data.polygonId == -1 || data.getCellBorder == true
  }

  val emptyPolygon: Polygon = {
    this.edge.getFactory.createPolygon(Array.empty[Coordinate])
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

  def getHedges(implicit geofactory: GeometryFactory): List[Half_edge] = {
    val coordinates = coords.map(_.coord)
    val pairs = coordinates.zip(coordinates.tail)
    pairs.map{ case(v1, v2) =>
      Half_edge(geofactory.createLineString(Array(v1, v2)))
    }.toList
  }

  def getCoords: Array[Coordinate] = {
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

object Segment {
  def save(segment: Segment)(implicit geofactory: GeometryFactory): String = {
    val line = segment.getLine // wkt isHole extremeToRemove
    val fid = segment.first.id
    val lid = segment.last.id

    s"$line\t$fid\t$lid"
  }

  def load(string: String)(implicit geofactory: GeometryFactory): Segment = {
    val arr = string.split("\t")
    val wkt = arr(0)
    val hole = arr(1).toInt
    val extr = arr(2)
    val fid = try{ arr(3).toLong } catch { case e: Throwable => -1L}
    val lid = try{ arr(4).toLong } catch { case e: Throwable => -1L}

    val reader = new WKTReader(geofactory)
    val coords = reader.read(wkt).asInstanceOf[LineString].getCoordinates
    val hedges = coords.zip(coords.tail).map{ case(v1, v2) =>
      val edge = geofactory.createLineString(Array(v1, v2))
      val hedge = Half_edge(edge)
      hedge
    }.toList

    val segment = Segment(hedges)
    segment.extremeToRemove = extr
    segment
  }
}

case class Segment(hedges: List[Half_edge]) {
  val first = hedges.head
  val last = hedges.last
  val polygonId = first.data.polygonId
  val ringId = first.data.ringId
  val startId = first.data.edgeId
  val endId = last.data.edgeId
  var extremeToRemove = "N"

  override def toString = s"${polygonId}:\n${hedges.map(_.edge)}"

  def wkt(implicit geofactory: GeometryFactory): String = {
    val coords = hedges.map{_.v1} :+ hedges.last.v2
    val s = geofactory.createLineString(coords.toArray)
    s"${s.toText}\t${first.data}\t${extremeToRemove}"
  }

  def toLine(implicit geofactory: GeometryFactory): String = {
      val coords = hedges.map{_.v1} :+ hedges.last.v2
      val s = geofactory.createLineString(coords.toArray)
      s"${s.toText}\t${isHole}\t${extremeToRemove}"
  }

  def getLine(implicit geofactory: GeometryFactory): String = {
    if(isClose){
      val coords = hedges.map{_.v1} :+ hedges.last.v2
      val s = geofactory.createLineString(coords.toArray)
      s"${s.toText}\t${isHole}\tN"      
    } else {
      extremeToRemove = 
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

  /*
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
   */

  def isHole: Int = if(hedges.exists(!_.data.isHole)) 0 else 1

  def isClose: Boolean = last.v2 == first.v1

  def getExtremes: List[Half_edge] = if(hedges.size == 1) List(first) else List(first, last)

  def tail: Segment = Segment(hedges.tail) 
}

case class Cell(id: Int, lineage: String, mbr: LinearRing){
  var lids: List[String] = List.empty[String]
  var hasCrossingEdges: Boolean = true
  val boundary = mbr.getEnvelopeInternal

  def getLids: List[String] = if(lids.isEmpty) List(lineage) else lids

  def wkt(implicit geofactory: GeometryFactory) = s"${toPolygon.toText}\tL${lineage}\t${id}"

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

    val ed1 = EdgeData(polyId, -1, 1, false, label); ed1.setCellBorder(true); e1.setUserData(ed1)
    val ed2 = EdgeData(polyId, -1, 2, false, label); ed2.setCellBorder(true); e2.setUserData(ed2)
    val ed3 = EdgeData(polyId, -1, 3, false, label); ed3.setCellBorder(true); e3.setUserData(ed3)
    val ed4 = EdgeData(polyId, -1, 4, false, label); ed4.setCellBorder(true); e4.setUserData(ed4)

    val tag = s"$label$polyId"
    val h1 = Half_edge(e1); h1.tag = tag; h1.id  = 0;
    val h2 = Half_edge(e2); h2.tag = tag; h2.id  = 1; 
    val h3 = Half_edge(e3); h3.tag = tag; h3.id  = 2; 
    val h4 = Half_edge(e4); h4.tag = tag; h4.id  = 3; 

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
