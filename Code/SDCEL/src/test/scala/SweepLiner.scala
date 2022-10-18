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

  private def getSentinels(dataset: List[Segment])(implicit geofactory: GeometryFactory): (Segment, Segment) = {
    val envelope = getEnvelope(dataset)
    envelope.expandBy(1.0)

    val low = mkSegment( (envelope.getMinX, envelope.getMinY), (envelope.getMaxX, envelope.getMinY), -1)
    val up  = mkSegment( (envelope.getMinX, envelope.getMaxY), (envelope.getMaxX, envelope.getMaxY), -2)
    (low, up)
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
    val s9 = mkSegment( (4,1), (4,6), 9)
    val s10 = mkSegment( (6,6), (6,9), 10)

    List(s1, s2, s3, s4, s5, s7, s8, s9, s10)
  }

  private def loadDataset2(implicit geofactory: GeometryFactory): List[Segment] = {
    val s1  = mkSegment(     (2, 8),    (5, 1), 1)
    val s2  = mkSegment(     (3, 1),    (5, 3), 2)
    val s3  = mkSegment(     (2, 1),    (5, 4), 3)
    val s4  = mkSegment(    (3, 10),    (5, 5), 4)
    val s5  = mkSegment(     (2, 3),    (5, 6), 5)
    val s6  = mkSegment(     (2, 5),    (5, 7), 6)
    val s7  = mkSegment(     (2, 9),    (5, 8), 7)
    val s8  = mkSegment(     (2, 7),    (5,10), 8)
    val s9  = mkSegment( (2.5, 0.5), (3.5,0.5), 9)
    val s10 = mkSegment(   (4, 0.5), (4.5,0.5), 10)

    List(s1, s2, s3, s4, s5, s7, s8, s6, s9, s10)
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

  private def readDataset(filename: String)(implicit geofactory: GeometryFactory): List[Segment] = {
    import scala.io.Source
    import com.vividsolutions.jts.io.WKTReader

    val reader = new WKTReader(geofactory)
    val buffer = Source.fromFile(filename)
    val segments = buffer.getLines().map{ line =>
      val arr = line.split("\t")
      val wkt = arr(0)
      val lab = arr(1).substring(0,1)
      val sid = arr(3).toLong
      val linestring = reader.read(wkt).asInstanceOf[LineString]
      val halfedge = Half_edge(linestring)
      halfedge.id = sid
      Segment(halfedge, lab)
    }.toList
    buffer.close()
    segments
  }

  private def saveStatusOrder(p: Coordinate)(implicit status: TreeMap[Segment, Segment]): Unit = {
    save(s"/tmp/edgesYO_${p.x.toString.replace(".", "-")}.wkt") {
      status.asScala.iterator.filter(_._1.id >= 0).map { x =>
        val s = x._1
        s"POINT( ${p.x} ${s.value} )\t${s.id}\t${s.value}\n"
      }.toList
    }
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

  private def updateY_order(sweep_point: Coordinate)
                           (implicit sweepComparator: SweepComparator, y_order: TreeMap[Segment, Segment]): Unit = {
    val ss = y_order.asScala.clone()
    y_order.clear()
    val sl = sweepComparator.setSweep(sweep_point)
    y_order.putAll(ss.asJava)
    print("")
  }

  private def updateY_order2(sweep_point: Coordinate, tolerance: Double, segs: Set[Segment] = Set.empty[Segment])
                           (implicit sweepComparator: SweepComparator, y_order: TreeMap[Segment, Segment]): Unit = {
    val ss = y_order.asScala.clone() ++ segs.map(x => x -> x)
    y_order.clear()
    val sl = sweepComparator.setSweep(sweep_point, tolerance)
    y_order.putAll(ss.asJava)
    print("")
  }

  private def succ(p: Coordinate, tolerance: Double = 0.001)
                  (implicit y_order: TreeMap[Segment, Segment], gf: GeometryFactory): Segment = {
    val query = mkSegment( (p.x, p.y), (p.x + tolerance, p.y + tolerance), -3) // dummy segment for query purposes...
    y_order.higherKey(query)
  }

  private def pred(p: Coordinate, tolerance: Double = 0.001)
                  (implicit y_order: TreeMap[Segment, Segment], gf: GeometryFactory): Segment = {
    val query = mkSegment((p.x, p.y), (p.x + tolerance, p.y + tolerance), -3) // dummy segment for query purposes...
    y_order.lowerKey(query)
  }

  /* Check if intersection has to be added to event queue */
  private def checkIntersection(intersection: Coordinate, sweep_point: Coordinate, ids: List[Long])
                               (implicit x_order: TreeMap[Coordinate, List[Event]]): Boolean = {
    // intersection is left to the sweep line or in it and above sweep point...
    val a = sweep_point.x < intersection.x || (sweep_point.x == intersection.x && sweep_point.y < intersection.y)
    // intersection is not in event queue...
    val b = if( x_order.containsKey(intersection) ){
      val events_ids = x_order.get(intersection).map{ event => event.segment.id }
      if(ids.map{ case id => events_ids.contains(id) }.reduce{ _ && _ }){
        // intersection is in event queue and it have the same segments...
        false
      } else {
        // intersection is in event queue but it does not have the same segments...
        true
      }
    } else {
      // intersection is not in event queue...
      true
    }
    a && b
  }

  def findNewEvent(seg_pred: Segment, seg_succ: Segment, point: Coordinate)
                  (implicit x_order: TreeMap[Coordinate, List[Event]]): Unit = {
    if(seg_pred.intersects(seg_succ)) {
      seg_pred.intersectionS(seg_succ) match {
        case Some(intersection) => {
          val a = point.x < intersection.x
          val b = point.x == intersection.x && point.y < intersection.y
          val c = !x_order.containsKey(intersection)
          if (checkIntersection(intersection, point, List(seg_pred.id, seg_pred.id))) {
            //println(s"Finding new event ($mode) at ${point} between ${seg_pred.id} and ${seg_succ.id}: ${intersection}")
            val event_pred = Event(intersection, seg_pred, "INTERSECTION")
            val event_succ = Event(intersection, seg_succ, "INTERSECTION")
            val events = if (x_order.containsKey(intersection)) {
              // if the intersection is already in the event queue we have to keep the possible previous segments...
              List(event_pred, event_succ) ++ x_order.get(intersection)
            } else {
              List(event_pred, event_succ)
            }
            x_order.put(intersection, events)
          }
        }
        case None => {
          //println(s"Finding new event ($mode) at ${point} between ${seg_pred.id} and ${seg_succ.id}")
        }
      }
    }
  }

  case class Event(point: Coordinate, segment: Segment, mode: String) {
    def isStart: Boolean = mode == "START"

    def isIntersection: Boolean = mode == "INTERSECTION"

    def isEnd: Boolean = mode == "END"

    def wkt: String = s"POINT( ${point.x} ${point.y} )\t${segment.id}\t${mode}"
  }

  def main(args: Array[String]): Unit = {
    val tolerance = 0.001
    implicit val model = new PrecisionModel(1.0 / tolerance)
    implicit val geofactory = new GeometryFactory(model)
    val debug: Boolean = true
    val generate: String = "read"
    val envelope_generate = new Envelope(0, 100, 0, 100)
    val n = 100
    val length = envelope_generate.getWidth * 0.25
    val filename = "/home/and/RIDIR/tmp/edgesS1.wkt"

    val dataset = generate match {
      case "sample" => loadDataset2
      case "random" => generateDataset(n, envelope_generate, length)
      case "read"   => readDataset(filename)
      case _ => loadDataset
    }
    val envelope = getEnvelope(dataset)
    saveSegments(dataset, "/tmp/edgesSS.wkt")
    implicit val x_order: TreeMap[Coordinate, List[Event]] = new TreeMap[Coordinate, List[Event]]
    implicit val sweepComparator = new SweepComparator(envelope, geofactory)
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
      save("/tmp/edgesXO.wkt") {
        x_order.asScala.iterator.zipWithIndex.map { case (event_point, i) =>
          val point = event_point._1
          val events = event_point._2.map { event =>
            val id = event.segment.id
            val mode = event.mode
            s"${id}_${mode}"
          }.mkString(", ")
          s"POINT( ${point.x} ${point.y} )\t${i}\t${events}\n"
        }.toList
      }
    }

    // Adding sentinels...
    val (s1, s2) = getSentinels(dataset)
    sweepComparator.setSweep(s1.source)
    y_order.put(s1, s1)
    y_order.put(s2, s2)

    val intersections = ListBuffer[String]()

    while (!x_order.isEmpty) {
      val event_point = x_order.pollFirstEntry()
      // Handle Event Point...
      val sweep_point = event_point.getKey
      val events = event_point.getValue
      val (u, c, l) = getUCL(events)

      if(c.map(_.id).contains(54) && c.map(_.id).contains(64)){
      //if(u.map(_.id).contains(54)){
        println()
      }

      if ((u union c union l).size > 1) {
        val segments = u union c union l
        val x = sweep_point.x
        val y = sweep_point.y
        val ids = segments.map{_.id}.toList.sorted.mkString(" ")
        val wkt = s"POINT($x $y)"
        intersections.append(s"$wkt\t$x\t$y\t$ids")
      }
      // Deleting L(p) U C(p)...
      updateY_order(sweep_point)
      (c union l).foreach { segment =>
        y_order.remove(segment)
      }
      // Inserting U(p) U C(p)...
      val uc = u union c
      updateY_order2(sweep_point, tolerance, uc)

      if ((u union c).size == 0) {
        val seg_pred = pred(sweep_point)
        val seg_succ = succ(sweep_point)
        findNewEvent(seg_pred, seg_succ, sweep_point)
      } else {
        val uc = (u union c).map{ seg =>
          y_order.get(seg)
        }
        val seg_prime = uc.minBy { _.value } // the lowest segment in U(p) U C(p)...
        val seg_pred = y_order.lowerKey(seg_prime)
        findNewEvent(seg_pred, seg_prime, sweep_point)
        val seg_prime_prime = uc.maxBy { _.value } // the highest segment in U(p) U C(p)...
        val seg_succ = y_order.higherKey(seg_prime_prime)
        findNewEvent(seg_prime_prime, seg_succ, sweep_point)
      }

      saveStatusOrder(sweep_point)
    }

    val I1 = intersections.map { i => s"$i\n" }.sorted

    save("/tmp/edgesI1.wkt") { I1 }

    // brute force validation
    val I2 = dataset.flatMap { segment =>
      val segments = dataset.filterNot(_.id == segment.id)
      segments.flatMap { segment_prime =>
        segment.intersection(segment_prime) match {
          case Some(intersection) => {
            val t1 = (intersection, segment.id)
            val t2 = (intersection, segment_prime.id)
            List(t1, t2)
          }
          case None => List.empty
        }
      }
    }.groupBy {
      _._1
    }.map { case (key, list) =>
      val x = key.x
      val y = key.y
      val wkt = s"POINT($x $y)"
      val ids = list.map(_._2).distinct.sorted.mkString(" ")
      s"$wkt\t$x\t$y\t$ids\n"
    }.toList.sorted

    save("/tmp/edgesI2.wkt") { I2 }

    val r = I1.zip(I2).map{ case(a, b) => a == b }.reduce( _ && _ )
    println(r)
  }
}

class SweepComparator(envelope: Envelope, geofactory: GeometryFactory) extends Comparator[Segment]{
  private var sweep: Coordinate = new Coordinate(Double.MinValue, Double.MinValue)
  var sweepline: LineString = computeSweepline()

  def setSweep(p: Coordinate, tolerance: Double = 0.0): LineString = {
    sweep = p
    sweepline = computeSweepline(tolerance)
    sweepline
  }

  def compare(s1: Segment, s2: Segment): Int = {
    s1.value = s1.findIntersection(sweepline).y
    s2.value = s2.findIntersection(sweepline).y

    s1.value.compare(s2.value)
  }

  def computeSweepline(gap: Double = 0.0): LineString = {
    val minY = envelope.getMinY - 2.0 // adding some gap to intersect sentinels...
    val maxY = envelope.getMaxY - 2.0 // adding some gap to intersect sentinels...
    val p1 = new Coordinate(sweep.x, minY)
    val p2 = new Coordinate(sweep.x + gap, maxY) // adding a small gap if we want to compute the
                                                 // intersection just after the sweepline...
    geofactory.createLineString(Array(p1, p2))
  }
}
