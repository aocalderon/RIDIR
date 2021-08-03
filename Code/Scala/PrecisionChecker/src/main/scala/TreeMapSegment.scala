import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.geom.{Coordinate, LineString}

import scala.collection.mutable.{TreeSet, Map}

case class Segment(point: Coordinate, seg: LineString){
  override def toString: String = s"(${point.x} ${point.y}) $seg}"
}

case class Event(point: Coordinate, segs: List[LineString], mode: String){
  override def toString: String = s"(${point.x} ${point.y}) $mode ${segs}"
}

object TreeSetSegment {

  def main(args:Array[String]) = {
    val scale = 1000.0
    val model = new PrecisionModel(scale)
    val geofactory = new GeometryFactory(model)

    val coords1 = Array(new Coordinate(0,7), new Coordinate(4,6))
    val s1 = geofactory.createLineString(coords1)
    val coords2 = Array(new Coordinate(1,5), new Coordinate(5,8))
    val s2 = geofactory.createLineString(coords2)
    val coords3 = Array(new Coordinate(2,4), new Coordinate(7,1))
    val s3 = geofactory.createLineString(coords3)
    val coords4 = Array(new Coordinate(3,5), new Coordinate(7,6))
    val s4 = geofactory.createLineString(coords4)
    val coords5 = Array(new Coordinate(5,3), new Coordinate(9,5))
    val s5 = geofactory.createLineString(coords5)
    val coords6 = Array(new Coordinate(6,4), new Coordinate(12,1))
    val s6 = geofactory.createLineString(coords6)
    val coords7 = Array(new Coordinate(3,1), new Coordinate(7,2))
    val s7 = geofactory.createLineString(coords7)
    val coords8 = Array(new Coordinate(3,5), new Coordinate(5,4))
    val s8 = geofactory.createLineString(coords8)

    val tree: TreeSet[LineString] = TreeSet.empty[LineString](LineStringOrdering)
    println("Original")
    tree.add(s7)
    tree.add(s3)
    tree.add(s4)
    tree.add(s2)
    tree.add(s6)
    tree.add(s8)
    tree.add(s1)
    tree.add(s5)
    tree.foreach(println)
    println(s"Size of tree: ${tree.size}")

    println("Search")
    def search(tree: TreeSet[LineString], key: LineString): List[LineString] = {
      val index = if(tree.contains(key)) tree.until(key).size else -1
      //println(index)
      if(index == -1){
        List.empty[LineString]
      } else {
        val lower = if(index - 1 < 0) 0 else index - 1
        val upper = if(index + 2 >= tree.size) tree.size else index + 2
        // need +2 because slice does not include last value...
        val subtree = tree.slice(lower, upper)
        subtree.toList
      }
    }
    val n = search(tree, s8)
    n.foreach(println)
    
    println("After remove")
    tree.remove(s8)
    tree.remove(s1)
    tree.foreach(println)

    val segments = List(s1,s2,s3,s4,s5,s6,s7,s8)
    val queue: TreeSet[Event] = TreeSet.empty[Event](EventOrdering)
    segments.flatMap{ segment =>
      val l = (getLeft(segment), segment, "L")
      val r = (getRight(segment), segment, "R")

      List(l,r)
    }.groupBy{case(coord, line, mode) => (coord, mode)}
      .map{ case((coord, mode), list) =>
        val segs = list.map(_._2)
        val event = Event(coord, segs, mode)
        queue.add(event)
    }
    queue.foreach(println)

    // Store points depending of they are leftend, rightend or interior points of a segment...
    var Lefts: Map[Coordinate, List[LineString]] = Map().withDefaultValue(List.empty[LineString])
    def addToLefts(coord: Coordinate, seg: LineString): Unit = Lefts.get(coord) match {
      case Some(xs: List[LineString]) => Lefts(coord) = xs :+ seg
      case None => Lefts(coord) = List(seg)
    }
    var Rights: Map[Coordinate, List[LineString]] = Map().withDefaultValue(List.empty[LineString])
    def addToRights(coord: Coordinate, seg: LineString): Unit = Rights.get(coord) match {
      case Some(xs: List[LineString]) => Rights(coord) = xs :+ seg
      case None => Rights(coord) = List(seg)
    }
    var Inters: Map[Coordinate, List[LineString]] = Map().withDefaultValue(List.empty[LineString])
    def addToInters(coord: Coordinate, seg: LineString): Unit = Inters.get(coord) match {
      case Some(xs: List[LineString]) => Inters(coord) = xs :+ seg
      case None => Inters(coord) = List(seg)
    }
/*
    // The status structure...
    val status: TreeSet[Segment] = TreeSet.empty[Segment](SegmentOrdering)
    for(event <- queue){
      if(event.mode == "L"){

        event.segs.foreach(seg => status.add(seg))
      } else if(event.mode == "R"){
        event.segs.foreach(seg => status.remove(seg))
      }
      println(s"Status at event $event")
      status.foreach(println)
    }
 */
  }

  def getLeft(line: LineString): Coordinate = {
    val start = line.getStartPoint
    val end = line.getEndPoint

    if(start.getX < end.getX)
      start.getCoordinate
    else
      end.getCoordinate
  }
  def getRight(line: LineString): Coordinate = {
    val start = line.getStartPoint
    val end = line.getEndPoint

    if(start.getX > end.getX)
      start.getCoordinate
    else
      end.getCoordinate
  }
}

object LineStringOrdering extends Ordering[LineString] {
  def compare(seg1:LineString, seg2:LineString) = {
    val xa1 = seg1.getStartPoint.getX
    val xa2 = seg1.getEndPoint.getX
    val xa = if(xa1 < xa2) xa1 else xa2

    val xb1 = seg2.getStartPoint.getX
    val xb2 = seg2.getEndPoint.getX
    val xb = if(xb1 < xb2) xb1 else xb2

    val ya1 = seg1.getStartPoint.getY
    val ya2 = seg1.getEndPoint.getY
    val ya = if(ya1 < ya2) ya1 else ya2

    val yb1 = seg2.getStartPoint.getY
    val yb2 = seg2.getEndPoint.getY
    val yb = if(yb1 < yb2) yb1 else yb2

    if(xa == xb) ya compare yb else xa compare xb
  }
}

object SegmentOrdering extends Ordering[Segment] {
  def compare(ev1:Segment, ev2:Segment) = {
    val y1 = ev1.point.y
    val y2 = ev2.point.y

    val x1 = ev1.point.x
    val x2 = ev2.point.x

    
    if(y1 == y2) x1 compare x2 else y1 compare y2
  }
}

object EventOrdering extends Ordering[Event] {
  def compare(ev1:Event, ev2:Event) = {
    val x1 = ev1.point.x
    val x2 = ev2.point.x

    val y1 = ev1.point.y
    val y2 = ev2.point.y
    
    if(x1 == x2) y1 compare y2 else x1 compare x2
  }
}
