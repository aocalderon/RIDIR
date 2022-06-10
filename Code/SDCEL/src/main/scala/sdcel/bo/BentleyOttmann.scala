package edu.ucr.dblab.bo3

import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel}
import com.vividsolutions.jts.geom.{Coordinate, LineString, Point}

import scala.collection.JavaConverters._
import scala.collection.mutable.{ListBuffer, ArrayBuffer}

import java.util.{PriorityQueue, TreeSet}

import edu.ucr.dblab.sdcel.geometries.Half_edge
import edu.ucr.dblab.sdcel.Utils.round

case class Intersection(p: Coordinate, seg1: Segment, seg2: Segment)
  (implicit geofactory: GeometryFactory) {

  val point: Point = geofactory.createPoint(p)

  val lines: List[LineString] = List(seg1.asJTSLine, seg2.asJTSLine)
}

case class Event(point: Coordinate, segments: List[Segment], ttype: Int)
  (implicit geofactory: GeometryFactory) extends Ordered[Event] {

  val value = if(ttype == 1 && segments(0).isVertical) // TODO: cover special case...
    point.x + 0.001
  else
    point.x

  override def compare(that: Event): Int = {
    if( this.value > that.value ) {
      1
    } else if( this.value < that.value ) {
     -1
    } else {
      this.point.y compare that.point.y
    }
  }

  private def L: String = s"LINESTRING($value 0, $value 1000 )"

  def asJTSLine: LineString = geofactory.createLineString(
    Array(new Coordinate(value, 0), new Coordinate(value, 1000))
  )

  def wkt: String = {
    val coords = s"${point.x} ${point.y}"
    val segs = segments.map{ seg => s"${seg.label}${seg.id}" }.mkString(" ")

    s"$L\t$value\t$coords\t$ttype\t$segs"
  }

  override def toString: String = {
    val coords = s"(${point.x} ${point.y})"
    val wkt = s"POINT${coords}"
    val segs = segments.map(_.id).mkString(", ")

    f"$wkt%-25s value: ${value} event_point: $coords type: $ttype segs_id: { $segs }"
  }
}

object Tree {
  implicit val geofactory = new GeometryFactory()

  def empty_seg(implicit geofactory: GeometryFactory): Segment = {
    val epsilon = 0.001
    val p1 = new Coordinate(Double.MinValue, Double.MinValue)
    val p2 = new Coordinate(Double.MinValue + epsilon, Double.MinValue)
    val fake_h = Half_edge(geofactory.createLineString(Array(p1, p2)))
    fake_h.id = Long.MinValue
    Segment(fake_h, "*")
  }
  val empty_node: Node = Node(Double.MinValue, ArrayBuffer(empty_seg))

  def node(segment: Segment)(implicit T: TreeSet[Node]): Node = T.floor( Node(segment.value) )

  def add(segment: Segment)(implicit T: TreeSet[Node]): Boolean = {
    val N = node(segment)
    if( N != null ){
      N.add(segment)
    } else {
      T.add(Node(segment.value, ArrayBuffer(segment)))
    }
  }

  def remove(segment: Segment)(implicit T: TreeSet[Node]): Boolean = {
    val N = node(segment)
    if(N != null){
      if(N.size > 1){
        N.remove(segment)
      } else {
        T.remove(N)
      }
    } else {
      false
    }
  }

  def swap(seg1: Segment, seg2: Segment)(implicit T: TreeSet[Node]): Unit = {
    remove(seg1)
    remove(seg2)
    val value  = seg1.value
    seg1.value = seg2.value
    seg2.value = value
    add(seg1)
    add(seg2)
  }

  def recalculate(L: Double)(implicit T: TreeSet[Node]): Iterator[Long] = {
    T.asScala.foreach{ node =>
      node.value = node.segments.head.calculateValue(L)
      node.segments.foreach{ seg => seg.value = node.value }
    }
    
    T.iterator.asScala.map{ _.segments.map(_.id) }.flatten
  }

  def higher(segment: Segment)(implicit T: TreeSet[Node]): Segment = {
    val N = node(segment)
    if(N != null){
      val segs = N.segments
      if( segment.id == segs.last.id ){ // Pick head in higher T Node...
        if( T.higher(N) != null) T.higher(N).segments.head else null
      } else { // Pick next Segment in current segs...
        val index = segs.indexWhere(_.id == segment.id)
        if(index < segs.size - 1) segs( index + 1 ) else null
      }
    } else {
      null
    }
  }

  def lower(segment: Segment)(implicit T: TreeSet[Node]): Segment = {
    val N = node(segment)
    if(N != null){
      val segs = N.segments
      if( segment.id == segs.head.id ){ // Pick last in lower T Node...
        if( T.lower(N) != null ) T.lower(N).segments.last else null
      } else { // Pick prev Segment in current segs...
        val index = segs.indexWhere(_.id == segment.id)
        if(index > 1) segs( index  - 1 ) else null
      }
    } else {
      null
    }
  }
}

case class Node(var value: Double,
  var segments: ArrayBuffer[Segment] = ArrayBuffer.empty[Segment]) extends Ordered[Node] {

  var id: Long = -1

  def add(seg: Segment)    = {
    if( !segments.exists(_.id == seg.id) ){
      segments.append(seg)
      sort
      true
    } else {
      false
    }
  }

  def remove(seg: Segment) = {
    val index = segments.indexWhere(_.id == seg.id)
    if( index >= 0 ){
      segments.remove(index)
      true
    } else {
      false
    }
  }

  def size: Int = segments.size

  def sort: Unit = { segments = segments.sortBy(_.angle) }

  def wkt: String = segments.map{ seg => s"${seg.wkt}\t${id}\t${value}\n" }.mkString("")

  override def compare(that: Node): Int = {
    -( this.value compare that.value )
  }

  override def toString: String = {
    segments.map{ segment =>
      s"node_id: $id \t node_value: ${round(value, 2)} \t segment: ${segment} "
    }.mkString("\n")
  }

}

case class Segment(h: Half_edge, label: String)
  (implicit geofactory: GeometryFactory) extends Ordered[Segment]{

  val p_1:   Coordinate = h.v1
  val p_2:   Coordinate = h.v2
  val id:    Long = h.id 
  val lid:   String = s"${label}${id}"
  val angle: Double = hangle(p_1, p_2)
  val source:   Coordinate = h.v1
  val target:   Coordinate = h.v2
  var value: Double = this.calculateValue(this.first.x)

  def dx: Double = target.x - source.x
  def dy: Double = target.y - source.y
  def slope: Double = dy / dx

  def first: Coordinate = {
    if( p_1.x < p_2.x ) { p_1 }
    else if( p_1.x > p_2.x ) { p_2 }
    else {
      if( p_1.y < p_2.y ) { p_1 }
      else { p_2 }
    }
  }
  
  def second: Coordinate = {
    if( p_1.x < p_2.x ) { p_2 }
    else if( p_1.x > p_2.x ) { p_1 }
    else {
      if( p_1.y < p_2.y ) { p_2 }
      else { p_1 }
    }
  }

  def calculateValue(value: Double): Double = {
    val x1 = this.first.x; val x2 = this.second.x
    val y1 = this.first.y; val y2 = this.second.y

    val dx = x2 - x1 // TODO: Track Zero division...
    val dy = y2 - y1

    val vx = value - x1
    
    y1 + ( (dy / dx) * vx ) // TODO: NaN value does not seem to affect...
  }

  def isVertical: Boolean = p_1.x == p_2.x

  def isHorizontal: Boolean = p_1.y == p_2.y
  
  private def hangle(p_1: Coordinate, p_2: Coordinate): Double = {
    val dx = p_1.x - p_2.x
    val dy = p_1.y - p_2.y
    val length = math.sqrt( (dx * dx) + (dy * dy) )
    val angle = if(dy > 0){
      math.acos(dx / length)
    } else {
      2 * math.Pi - math.acos(dx / length)
    }
    math.toDegrees(angle)
  }

  def asJTSLine: LineString = {
    val line = geofactory.createLineString(Array(p_1, p_2))
    line.setUserData(s"$label\t$id\t$value")
    line
  }

  def wkt: String = s"${asJTSLine.toText}\t${label}${id}\t$value"

  override def compare(that: Segment): Int = this.value compare that.value

  override def toString: String =
    f"${asJTSLine.toText}%-30s label: ${label}%-4s id: ${id}%-3s value: ${this.value}%-20s"
}

object BentleyOttmann {
  implicit val model: PrecisionModel = new PrecisionModel(1e-3)
  implicit val geofactory: GeometryFactory = new GeometryFactory(model)
  implicit var T: TreeSet[Node]   = new TreeSet[Node]()
  var Q: PriorityQueue[Event]     = new PriorityQueue[Event]()
  var X: ListBuffer[Intersection] = new ListBuffer[Intersection]()

  def readSegments(input_data: List[Segment]): Unit = {
    input_data.foreach { s =>
      this.Q.add(Event(s.first,  List(s), 0))
      this.Q.add(Event(s.second, List(s), 1))
    }
  }

  //////////////////////////////////////// Primitives [Start] ////////////////////////////////////////

  val NO_IDEA = 2

  // Based on https://www.geeksforgeeks.org/orientation-3-ordered-points/
  // To find orientation of ordered triplet (p1, p2, p3). The function returns
  // following values:
  // -1 --> Clockwise
  //  0 --> p, q and r are collinear
  //  1 --> Counterclockwise
  def orientation(p1: Coordinate, p2: Coordinate,p3: Coordinate): Int = {
    // See 10th slides from following link for derivation of the formula...
    // http://www.dcs.gla.ac.uk/~pat/52233/slides/Geometry1x1.pdf
    val value = (p2.y - p1.y) * (p3.x - p2.x) - (p2.x - p1.x) * (p3.y - p2.y)

    value match {
      case x if x <  0 =>  1  //  counterclock wise
      case x if x == 0 =>  0  //  collinear
      case x if x  > 0 => -1  //  clock wise
    }
  }

  def oritentation(s: Segment, p: Coordinate): Int = {
    orientation(s.h.v1, s.h.v2, p)
  }

  private def sign(x: Double): Int = {
    x match {
      case _ if x <  0 => -1  
      case _ if x == 0 =>  0  
      case _ if x  > 0 =>  1
      case _ => NO_IDEA
    }
  }

  def cmp_slopes(s1: Segment, s2: Segment): Int = sign(s1.slope - s2.slope)

  /* See section 20 at (Mehlhorn and Naher, 1994) */
  def compareSegments(s1: Segment, s2: Segment, p_sweep: Coordinate): Int = {
    cmp_segments(
      s1.source.x, s1.source.y,
      s2.source.x, s2.source.y,
      s2.target.x, s2.target.y,
      p_sweep.x, p_sweep.y,
      s1.dx, s1.dy,
      s2.dx, s2.dy
    )
  }

  /* See section 19 at (Mehlhorn and Naher, 1994) */
  private def cmp_segments(
     px: Double,  py: Double, // s1.source
    spx: Double, spy: Double, // s2.source
    sqx: Double, sqy: Double, // s2.target
     rx: Double,  ry: Double, // p_sweep (sweepline's current point)
     dx: Double,  dy: Double, // s1 delta x and y
    sdx: Double, sdy: Double  // s2 delta x and y
  ): Int = {

    /* Segments are identical */
    val sign1 = sign( dy * sdx - sdy *  dx)
    val areIdentical = if( sign1 == 0 || sign1 == NO_IDEA ){
      val mdx = sqx - px
      val mdy = sdy - py
      val sign2 = sign( dy * mdx - mdy *  dx)
      if( sign2 == 0 || sign2 == NO_IDEA ){
        val sign3 = sign(sdy * mdx - mdy * sdx)
        if( sign3 == 0 || sign3 == NO_IDEA ){
          if( sign1 == 0 && sign2 == 0 && sign3 == 0 ) {
            Some(0)
          } else {
            Some(NO_IDEA)
          }
        } else {
          None
        }
      } else {
        None
      }
    } else {
      None
    }
    
    areIdentical match {
      case Some(i) => i
      case None => { /* The underlaying segments are different */
        if( dx == 0 ) {  // if s1 is vertical...
          val a = spy * sdx - spx * sdy
          val b = sdy *  rx -  ry * sdx
          val i = sign( a + b )
          i match {
            case x if i <= 0 =>  1
            case x if i  > 0 => -1
            case _ => NO_IDEA
          }
        } else if( sdx == 0) { // if s2 is vertical...
          val a = py * dx - px * dy
          val b = dy * rx - ry * dx
          val i = sign( a + b )
          i match {
            case x if i <= 0 =>  1
            case x if i  > 0 => -1
            case _ => NO_IDEA
          }
        } else { // neither s1 nor s2 is vertical...
          val a = sdx * (  py *  dx +  dy * ( rx -  px) )
          val b =  dx * ( spy * sdx + sdy * ( rx - spx) )
          val sign2 = sign( a - b )
          if( sign2 == NO_IDEA ) NO_IDEA
          else if( sign2 != 0 ) sign2
          else {
            val c = py * dx - px * dy
            val d = dy * rx - ry * dx
            val sign3 = sign( c + d )
            sign3 match {
              case x if sign3 <= 0 =>  sign1
              case x if sign3  > 0 => -sign1
              case _ => NO_IDEA
            }
          }
        }
      }
    }


  }

  //////////////////////////////////////// Primitives [End] ////////////////////////////////////////

  def getIntersections(implicit geofactory: GeometryFactory): List[Intersection] = {
    findIntersections
    this.X.toList
  }

  def printStatus(filter: String = "*") = {
    filter match {
      case "*" => this.T.iterator().asScala.zipWithIndex
          .map{ case(s, x)  => s"$x\t$s" }.foreach{ println }
      case _   => this.T.iterator().asScala.filter(_.segments.head.label != filter).zipWithIndex
          .map{ case(s, x)  => s"$s\t$x" }.foreach{ println }
    }
    println
  }

  def findIntersections(implicit geofactory: GeometryFactory): Unit = {
    var j = 0
    val f = new java.io.PrintWriter("/tmp/edgesQQ.wkt")
    val g = new java.io.PrintWriter("/tmp/edgesTT.wkt")

    while(!this.Q.isEmpty /* && j < 100 */ ) {
      j = j + 1
      val e: Event  = this.Q.poll()
      val L: Double = e.value

      f.write(s"${e.wkt}\t$j\n")

      e.ttype match {
        case 0 => {
          for{ s <- e.segments }{ 
            Tree.recalculate(L)
            val node = Node(s.value, ArrayBuffer(s))
            this.T.add(node)

            if( Tree.lower(s) != null ) {
              val r: Segment = Tree.lower(s)
              this.reportIntersection(r, s, L)
            }
            if( Tree.higher(s) != null ) {
              val t: Segment = Tree.higher(s)
              this.reportIntersection(t, s, L)
            }
            if( Tree.lower(s) != null && Tree.higher(s) != null ) {
              val r: Segment = Tree.lower(s)
              val t: Segment = Tree.higher(s)
              this.removeFuture(r, t);
            }

            T.iterator().asScala.foreach{ node =>
              val wkt = node.segments.map{ s =>
                s"POINT($L ${node.value})\t${s.label}${s.id}\t$j"
              }.mkString("\n")
              g.write(s"$wkt\n")
            }

          }
        }
        case 1 => {
          for{ s <- e.segments }{
            if(s.isVertical){

              println(j)
              println(s)
              printStatus(s.label)

            }
            if( Tree.lower(s) != null && Tree.higher(s) != null ) {
              val r: Segment = Tree.lower(s)
              val t: Segment = Tree.higher(s)
              this.reportIntersection(r, t, L)
            }

            T.iterator().asScala.foreach{ node =>
              val wkt = node.segments.map{ s =>
                s"POINT($L ${node.value})\t${s.label}${s.id}\t$j"
              }.mkString("\n")
              g.write(s"$wkt\n")
            }

            Tree.remove(s)
          }
        }
        case 2 => {
          val s_1: Segment = e.segments(0)
          val s_2: Segment = e.segments(1)
          Tree.swap(s_1, s_2)
          if( s_1.value < s_2.value ) {
            if(Tree.higher(s_1) != null) {
              val t: Segment = Tree.higher(s_1)
              this.reportIntersection(t, s_1, L)
              this.removeFuture(t, s_2)
            }
            if(Tree.lower(s_2) != null) {
              val r: Segment = Tree.lower(s_2)
              this.reportIntersection(r, s_2, L)
              this.removeFuture(r, s_1)
            }
          } else {
            if(Tree.higher(s_2) != null) {
              val t: Segment = Tree.higher(s_2)
              this.reportIntersection(t, s_2, L)
              this.removeFuture(t, s_1)
            }
            if(Tree.lower(s_1) != null) {
              val r: Segment = Tree.lower(s_1)
              this.reportIntersection(r, s_1, L)
              this.removeFuture(r, s_2)
            }
          }

          Tree.recalculate(L)
          T.iterator().asScala.foreach{ node =>
            val wkt = node.segments.map{ s =>
              s"POINT($L ${node.value})\t${s.label}${s.id}\t$j"
            }.mkString("\n")
            g.write(s"$wkt\n")
          }

          if( s_1.label != s_2.label ) {
            this.X.append(Intersection(e.point, s_1, s_2))
          }

        }
      }
    }
    f.close()
    g.close()
  }

  def reportIntersection(s_1: Segment, s_2: Segment, L: Double): Unit = {
    val x1 = s_1.first.x
    val y1 = s_1.first.y
    val x2 = s_1.second.x
    val y2 = s_1.second.y

    val x3 = s_2.first.x
    val y3 = s_2.first.y
    val x4 = s_2.second.x
    val y4 = s_2.second.y
    
    val r = (x2 - x1) * (y4 - y3) - (y2 - y1) * (x4 - x3)

    if( r != 0 ) {
      val t = ((x3 - x1) * (y4 - y3) - (y3 - y1) * (x4 - x3)) / r
      val u = ((x3 - x1) * (y2 - y1) - (y3 - y1) * (x2 - x1)) / r
      
      if( t >= 0 && t <= 1 && u >= 0 && u <= 1 ) { // Find intersection point...
        val x_c = x1 + t * (x2 - x1)
        val y_c = y1 + t * (y2 - y1)

        if( L < x_c ) { // Right to the sweep line...
	  val point = new Coordinate(x_c, y_c)
	  val segs  = List(s_1, s_2)
	  // Add to scheduler...
	  this.Q.add(Event(point, segs, 2))
        }
      }
    }
  }

  def removeFuture(s_1: Segment, s_2: Segment): Unit = {
    val event = this.Q.asScala.filter(_.ttype == 2).find{ e =>
      (e.segments(0) == s_1 && e.segments(1) == s_2) ||
      (e.segments(0) == s_2 && e.segments(1) == s_1)
    }
    event match {
      case Some(e) => this.Q.remove(e)
      case None    =>  
    }
  }
}
