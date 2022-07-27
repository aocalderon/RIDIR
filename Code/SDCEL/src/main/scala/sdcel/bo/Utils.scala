package edu.ucr.dblab.bo3

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, LineString, Point}
import edu.ucr.dblab.sdcel.Utils.logger
import edu.ucr.dblab.sdcel.geometries.Half_edge
import org.jgrapht.graph.DefaultEdge

import java.util
import java.util.{Comparator, TreeMap, TreeSet}
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/* Case class to model global settings */
case class Settings(
  debug: Boolean = false,
  embed: Boolean = false,
  use_optimization: Boolean = false,
  tolerance: Double = 1e-3,
  geofactory: GeometryFactory = new GeometryFactory()
)

/* Case class to model a sequential item (seq_item) suitable for the X and Y strucrutes */
/* LEDA book pag 138-141 */
object Structures extends Enumeration {
  type Structure = Value
  val X, Y = Value
}

import edu.ucr.dblab.bo3.Structures._

case class Key(key: Any, structure: Structure = null){
  def getKeyPoint: Coordinate = if(key.isInstanceOf[Point])
    key.asInstanceOf[Coordinate]
  else
    null

  def getKeySegment: Segment = key match {
    case segment: Segment => segment
    case _ => null
  }

  def getInfo(implicit X_structure: TreeMap[Coordinate, Seq_item],
    Y_structure: TreeMap[Segment, Seq_item]): Seq_item = {

    structure match {
      case X => X_structure.get(getKeyPoint).inf
      case Y => Y_structure.get(getKeySegment).inf
    }
  }

  def getKey: (Structure, Coordinate, Segment) = {
    structure match {
      case X => (X, getKeyPoint, null)
      case Y => (Y, null, getKeySegment)
    }
  }
}

case class Seq_item(key: Key, var inf: Seq_item = null)

/***************************************/
/*** START: Status compare functions ***/
/***************************************/
/* Java class extending Comparator for ordering segments in status (Y_structure) */
/* As stated at LEDA book pag 742 */
class sweep_cmp() extends Comparator[Segment]{
  private var sweep: Coordinate = new Coordinate(Double.MinValue, Double.MinValue)

  def setPosition(p: Coordinate): Unit = { sweep = p }

  def sign(x: Long): Int = x match {
    case _ if x <  0 => -1
    case _ if x == 0 =>  0
    case _ if x  > 0 =>  1
  }

  def compare(s1: Segment, s2: Segment): Int = {
    // Precondition:
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
          logger.error("Error in sweep_cmp -> compare !!!")
          compareIntersectionsWithSweepline(s1, s2, sweep)
        }
      }

      // check if one of the segments is trivial (LEDA book pag 743)...
      if(s >= 0 || s1.isTrivial(sweep) || s2.isTrivial(sweep)){
        s
      } else {
        val r = BentleyOttmann.orientation(s2, s1.target)
        // overlapping segments will be ordered by their ids...
        if( r >= 0 ) r else sign(s1.id - s2.id)
      }
    }
  }

  private def compareIntersectionsWithSweepline(s1: Segment, s2: Segment, sweep: Coordinate): Int = {
    var env = s1.envelope
    env.expandToInclude(s2.envelope)
    val coordinates = Array( new Coordinate(sweep.x, env.getMinY), new Coordinate(sweep.x, env.getMaxY) )
    val line = s1.h.edge.getFactory.createLineString(coordinates)

    try {
      val y1 = line.intersection(s1.asJTSLine).getCentroid.getY
      val y2 = line.intersection(s2.asJTSLine).getCentroid.getY
      y1 compare y2
    } catch {
      case e: Exception =>
        logger.error("s1 or s2 does not intersect with sweepline!!!")
        -1
    }
  }
}

/* Custom implementation for segment comparator for the status data structure */
/* Just for debugging purposes */
class sweep_cmp2() extends Comparator[Segment]{
  private var sweep: Coordinate = new Coordinate(Double.MinValue, Double.MinValue)

  def setSweep(p: Coordinate): Unit = { sweep = p }

  def compare(s1: Segment, s2: Segment): Int = {
    if(s1.identical(s2)){
      s1.id compare s2.id
    } else {
      val sweepline = getSweepline(s1, s2, sweep)
      val y1 = s1.intersectionY(sweepline)
      val y2 = s2.intersectionY(sweepline)
      y1 compare y2
    }
  }

  private def getSweepline(s1: Segment, s2: Segment, sweep: Coordinate): LineString = {
    val env = s1.envelope
    env.expandToInclude(s2.envelope)
    val coordinates = Array( new Coordinate(sweep.x, env.getMinY), new Coordinate(sweep.x, env.getMaxY) )
    s1.h.edge.getFactory.createLineString(coordinates)
  }
}
/*************************************/
/*** END: Status compare functions ***/
/*************************************/

/* Just a case class to model an intersection point */
case class Intersection(p: Coordinate, seg1: Segment, seg2: Segment)
  (implicit geofactory: GeometryFactory) {

  val point: Point = geofactory.createPoint(p)

  val lines: List[LineString] = List(seg1.asJTSLine, seg2.asJTSLine)
}

/* Support class to model the Graph's type node */
/* Follows LEDA book pag 743 */
case class SegmentEdge(segment: Segment) extends DefaultEdge {
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
  implicit val geofactory: GeometryFactory = new GeometryFactory()

  def empty_seg(implicit geofactory: GeometryFactory): Segment = {
    val epsilon = 0.001
    val p1 = new Coordinate(Double.MinValue, Double.MinValue)
    val p2 = new Coordinate(Double.MinValue + epsilon, Double.MinValue)
    val fake_h = Half_edge(geofactory.createLineString(Array(p1, p2)))
    fake_h.id = Long.MinValue
    Segment(fake_h, "*")
  }
  val empty_node: Node = Node(Double.MinValue, ArrayBuffer(empty_seg))

  def node(segment: Segment)(implicit T: util.TreeSet[Node]): Node = T.floor( Node(segment.value) )

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
 
