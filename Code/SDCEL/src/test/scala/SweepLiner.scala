package edu.ucr.dblab.sweeptest

import com.vividsolutions.jts.geom.{Coordinate, Envelope, GeometryFactory, LineString, PrecisionModel}
import edu.ucr.dblab.sdcel.geometries.Half_edge
import sdcel.bo.Segment
import edu.ucr.dblab.sdcel.Utils.save

import java.util.Comparator
import java.util.TreeMap
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.Random

object SweepLiner {
  private def mkSegment(p1: (Double, Double), p2: (Double, Double), id: Long)
                       (implicit geofactory: GeometryFactory): Segment = {
    val c1 = new Coordinate(p1._1, p1._2)
    val c2 = new Coordinate(p2._1, p2._2)
    val line = geofactory.createLineString(Array(c1, c2))
    val h = Half_edge(line);
    h.id = id
    Segment(h, "A")
  }

  private def getEnvelope(segments: List[Segment]): Envelope = {
    val minx = segments.minBy(_.envelope.getMinX).envelope.getMinX
    val maxx = segments.maxBy(_.envelope.getMaxX).envelope.getMaxX
    val miny = segments.minBy(_.envelope.getMinY).envelope.getMinY
    val maxy = segments.maxBy(_.envelope.getMaxY).envelope.getMaxY
    new Envelope(minx, maxx, miny, maxy)
  }

  private def saveSegments(dataset: List[Segment], filename: String): Unit = {
    save(filename) {
      dataset.map { s =>
        s"${s.wkt}\n"
      }
    }
    println(s"Saved ${dataset.size} records.")
  }

  private def loadDataset(implicit geofactory: GeometryFactory): List[Segment] = {
    val s1 = mkSegment( (2,4), (10,8), 1)
    val s2 = mkSegment( (6,6), (10,5), 2)
    val s3 = mkSegment( (3,9), (8,4), 3)
    val s4 = mkSegment( (3,2), (6,6), 4)
    val s5 = mkSegment( (5,2), (6,6), 5)
    val s7 = mkSegment( (4,1), (8,2), 7)
    val s8 = mkSegment( (4,9), (8,8), 8)

    List(s1, s2, s3, s4, s5, s7, s8)
  }
  private def generateDataset(n: Int, envelope: Envelope, length: Double = 100)
                             (implicit geofactory: GeometryFactory): List[Segment] = {
    val x1 = envelope.getMinX
    val y1 = envelope.getMinY
    (0 until n).map { i =>
      val x = x1 + Random.nextDouble() * envelope.getWidth
      val y = y1 + Random.nextDouble() * envelope.getHeight
      val x_prime = x + Random.nextDouble() * length
      val y_prime = y + Random.nextDouble() * length
      val p1 = new Coordinate(x, y)
      val p2 = new Coordinate(x_prime, y_prime)
      val line = geofactory.createLineString(Array(p1, p2))
      val halfedge = Half_edge(line)
      halfedge.id = i
      Segment(halfedge, "A")
    }.toList
  }

  private def getStatusOrder(implicit status: TreeMap[Segment, Segment]): String = {
    status.asScala.iterator.map {
      _._1.id
    }.mkString(" ")
  }

  private def getUCL(events: List[Event]): (Set[Segment], Set[Segment], Set[Segment]) = {
    @tailrec
    def ucl(events: List[Event],
            U: Set[Segment],
            C: Set[Segment],
            L: Set[Segment]
           ): (Set[Segment], Set[Segment], Set[Segment]) = {
      events match {
        case Nil => (U, C, L)
        case head :: tail => {
          val (u, c, l) = head.mode match {
            case "START"        => (U + head.segment, C, L)
            case "INTERSECTION" => (U, C + head.segment, L)
            case "END"          => (U, C, L + head.segment)
          }
          ucl(tail, u, c, l)
        }
      }
    }
    val U = Set.empty[Segment]
    val C = Set.empty[Segment]
    val L = Set.empty[Segment]
    ucl(events, U, C, L)
  }

  case class Event(point: Coordinate, segment: Segment, mode: String) {
    def isStart: Boolean = mode == "START"

    def isIntersection: Boolean = mode == "INTERSECTION"

    def isEnd: Boolean = mode == "END"

    def wkt: String = s"POINT( ${point.x} ${point.y} )\t${segment.id}\t${mode}"
  }

  def main(args: Array[String]): Unit = {
    implicit val model = new PrecisionModel(1000.0)
    implicit val geofactory = new GeometryFactory(model)
    val debug: Boolean = true
    val generate: Boolean = false
    val envelope = new Envelope(0, 100, 0, 100)
    val n = 20
    val length = envelope.getWidth * 0.1

    val dataset = if(generate) generateDataset(n, envelope, length) else loadDataset
    saveSegments(dataset, "/tmp/edgesSS.wkt")
    val sweepComparator = new SweepComparator(envelope, geofactory)
    implicit val x_order: TreeMap[Coordinate, List[Event]] = new TreeMap[Coordinate, List[Event]]
    implicit val y_order: TreeMap[Segment, Segment] = new TreeMap[Segment, Segment](sweepComparator)

    dataset.flatMap { segment =>
      val start = Event(segment.source, segment, "START")
      val end = Event(segment.target, segment, "END")
      List(start, end)
    }.groupBy(_.point).foreach { point_event =>
      val point = point_event._1
      val events = point_event._2
      x_order.put(point, events)
    }

    if (debug) {
      save("/tmp/edgesXO.wkt"){
        x_order.asScala.iterator.zipWithIndex.map{ case(event_point, i) =>
          val point = event_point._1
          val events = event_point._2.map{ event =>
            val id = event.segment.id
            val mode = event.mode
            s"${id}_${mode}"
          }.mkString(", ")
          s"POINT( ${point.x} ${point.y} )\t${i}\t${events}\n"
        }.toList
      }
    }

    while(!x_order.isEmpty){
      val event_point = x_order.pollFirstEntry()
      // Handle Event Point...
      val sweep_point = event_point.getKey
      println(sweep_point)
      val events = event_point.getValue
      val (u,c,l) = getUCL(events)
      if( (u union c union l).size > 1 ){
        val segments = u union c union l
        println(s"POINT( ${sweep_point.x} ${sweep_point.y} )\t${segments.map{_.id}.mkString(" ")}")
      }
    }
/*
    // brute force validation
    val status2 = new ListBuffer[String]
    (0.0 to 100.0 by 10.0).foreach { x =>
      val c1 = new Coordinate(x, 0.0)
      val c2 = new Coordinate(x, 100.0)
      val line = geofactory.createLineString(Array(c1, c2))
      val order = dataset.map { segment =>
        val p = line.intersection(segment.line).getCentroid
        (p, segment.id)
      }.sortBy {
        _._1.getY
      }.map {
        _._2
      }.mkString(" ")
      status2.append(order)
    }
*/
  }
}

class SweepComparator(envelope: Envelope, geofactory: GeometryFactory) extends Comparator[Segment]{
  private var sweep: Double = Double.MinValue

  def setSweep(p: Double): Unit = {
    sweep = p
  }

  def compare(s1: Segment, s2: Segment): Int = {
    val r = y_intersect(s1).compare(y_intersect(s2))
    if(r == 0){
      s1.id compare s2.id
    } else {
      r
    }
  }

  private def y_intersect(s: Segment): Double = {
    sweepline.intersection(s.line).getCoordinates.head.y
  }

  private def sweepline: LineString = {
    val p1 = new Coordinate(sweep, envelope.getMinY)
    val p2 = new Coordinate(sweep, envelope.getMaxY)
    geofactory.createLineString(Array(p1, p2))
  }
}
