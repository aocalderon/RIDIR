package edu.ucr.dblab.bo3

import com.vividsolutions.jts.geom.{GeometryFactory, Coordinate, Point, LineString}

import org.jgrapht.graph.DefaultEdge

import edu.ucr.dblab.sdcel.geometries.Half_edge
import edu.ucr.dblab.sdcel.Utils.logger

import scala.collection.JavaConverters._
import scala.collection.mutable.{ListBuffer, ArrayBuffer}

import java.util.{PriorityQueue, TreeSet, Comparator}

/* Java class extending Comparator for ordering segments in status (Y_structure) */
/* As stated at LEDA book pag 742 */
class sweep_cmp() extends Comparator[Segment]{
  var sweep: Coordinate = new Coordinate(Double.MinValue, Double.MinValue)

  def sign(x: Long): Int = x match {
    case _ if x <  0 => -1
    case _ if x == 0 =>  0
    case _ if x  > 0 =>  1
  }

  def compare(s1: Segment, s2: Segment): Int = {
    // Precondition
    // sweep is identical to the left endpoint of either s1 or s2...

    if(s1.identical(s2)){
      0
    } else {
      val s = if( sweep.equals(s1.source) ){
        BentleyOttmann.orientation(s2, sweep)
      } else {
        if( sweep.equals(s2.source) ){
          BentleyOttmann.orientation(s1, sweep)
        } else {
          logger.error("compare error in sweep!!!")
          System.exit(1)
          0
        }
      }

      // check if one of the segments is trivial (LEDA book pag 743)...
      if(s >= 0 || s1.isTrivial(sweep) || s2.isTrivial(sweep)){
        s
      } else {
        val r = BentleyOttmann.orientation(s2, s1.target)
        if( r >= 0 ) r else sign(s1.id - s2.id)
      }
    }
  }
}

/* Just a case class to model an intersection point */
case class Intersection(p: Coordinate, seg1: Segment, seg2: Segment)
  (implicit geofactory: GeometryFactory) {

  val point: Point = geofactory.createPoint(p)

  val lines: List[LineString] = List(seg1.asJTSLine, seg2.asJTSLine)
}

/* Support class to model the Graph's type node */
/* Follows LEDA book pag 743 */
case class NNode(segment: Segment) extends DefaultEdge {
  override def toString: String =
    s"source: ${this.getSource}\ttarget: ${this.getTarget}\tsegment: $segment"
}

/* Compare class to order the priority queue asked at LEDA book pag 743 */
class segmentByXY() extends Comparator[Segment]{
  def compare(s1: Segment, s2: Segment): Int = {
    val C = s1.first.x compare s2.first.x

    val R = C match{
      case 0 => s1.second.y compare s2.second.y
      case _ => C
    }

    R
  }
}

/* Tree object to model a data structure to support multiple segments per node */
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

/* Support node class for the previous Tree class */
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
      s"node_id: $id \t node_value: $value \t segment: ${segment} "
    }.mkString("\n")
  }

}
 
