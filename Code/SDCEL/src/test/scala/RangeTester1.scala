package edu.ucr.dblab.sweeptest

import com.vividsolutions.jts.geom._
import com.vividsolutions.jts.index.strtree.STRtree
import edu.ucr.dblab.sdcel.Utils.{envelope2WKT, save}
import org.scalatest.flatspec._
import org.scalatest.matchers._
import sdcel.bo._

import java.util.TreeMap
import scala.collection.JavaConverters._
import YStructure_Tester4.{EndPoint, getX_orderBySegment, readSegments}
import edu.ucr.dblab.sweeptest.RangeTester1.Interval

import scala.collection.mutable.ListBuffer

object RangeTester1 extends AnyFlatSpec with should.Matchers {

  def getEnvelope(segments: List[Segment]): Envelope = {
      segments.map(_.envelope).reduce{ (a,b) =>
        a.expandToInclude(b)
        a
      }
  }

  case class EndPoint(point: Coordinate, segment: Segment, isStart: Boolean)
  def getX_order(segments: List[Segment]): TreeMap[Coordinate, EndPoint] = {
    val x_order = new TreeMap[Coordinate, EndPoint]()
    segments.map { segment =>
      val a = EndPoint(segment.source, segment, true)
      val b = EndPoint(segment.target, segment, false)
      List(a, b)
    }.flatten.foreach { endpoint =>
      x_order.put(endpoint.point, endpoint)
    }
    x_order
  }

  def getX_orderBySegment(segments: List[Segment]): TreeMap[Coordinate, Segment] = {
    val x_order = new TreeMap[Coordinate, Segment]()
    segments.map { segment =>
      x_order.put(segment.source, segment)
    }
    x_order
  }
  def getSubsetsByIndex(big_dataset: List[Segment], intervals: List[Interval]): List[List[Segment]] = {
    // Feeding index...
    val index = new STRtree()
    big_dataset.foreach { segment =>
      index.insert(segment.envelope, segment)
    }

    // Querying index
    val big_envelope = getEnvelope(big_dataset)
    val miny = big_envelope.getMinY
    val maxy = big_envelope.getMaxY
    val subsets = intervals.map { interval =>
      val minx = interval.range.l
      val maxx = interval.range.r
      val envelope = new Envelope(minx, maxx, miny, maxy)
      index.query(envelope).asScala.map(_.asInstanceOf[Segment]).toList
    }

    subsets
  }

  def getSubsetsBySweep(big_dataset: List[Segment], intervals: List[Interval]): List[List[Segment]] = {
    val x_order = getX_order(big_dataset)
    val subsets = intervals.map { interval =>
      val y_order: TreeMap[Long, Segment] = new TreeMap[Long, Segment]()
      val xl = interval.range.l
      val xr = interval.range.r
      var endpoint: EndPoint = x_order.firstEntry().getValue
      // sweep until the left side of the range...
      while (endpoint.point.x <= xl) {
        endpoint = x_order.pollFirstEntry().getValue
        if (endpoint.isStart) {
          y_order.put(endpoint.segment.id, endpoint.segment) // add segment...
        } else {
          y_order.remove(endpoint.segment.id) // remove it because it doesn't touch the range...
        }
      }
      // sweep until the right side of the range...
      while (endpoint.point.x <= xr) {
        endpoint = x_order.pollFirstEntry().getValue
        if (endpoint.isStart) {
          y_order.put(endpoint.segment.id, endpoint.segment) // just add segment...
        } else {
          // we do not need remove segments during the range...
        }
      }

      y_order.asScala.iterator.map{ _._2 }.toList
    }

    subsets
  }

  def getSubsetsBySweep2(big_dataset: List[Segment], intervals: List[Interval]): List[List[Segment]] = {
    val x_order = getX_orderBySegment(big_dataset)
    val envelope = getEnvelope(big_dataset)

    // Feeding index...
    val index = new STRtree()
    big_dataset.foreach { segment =>
      index.insert(segment.envelope, segment)
    }

    val subsets = intervals.map { interval =>
      val left = interval.range.l
      val right = interval.range.r

      // Querying the index to get segments intersection left side of the range...
      // These will be segments starting before and entering the current interval...
      val query = new Envelope(left, left, envelope.getMinY, envelope.getMaxY)
      val subset_before = index.query(query).asScala.map(_.asInstanceOf[Segment]).toList

      // Querying the segments inside the interval.  These will be segments starting inside the interval...
      val left_coord = new Coordinate(left, 0.0)
      val right_coord = new Coordinate(right, 0.0)
      val start = x_order.ceilingKey(left_coord)
      val end = x_order.floorKey(right_coord)
      // Extracting segments from x_order between left and right coordinates from the interval...
      val subset_after = x_order.subMap(start, true, end, true)
        .asScala.values.toList

      // Adding to the final result...
      subset_before ++ subset_after
    }

    subsets
  }

  case class Range(l: Double, r: Double)
  case class Interval(range: Range, subset: List[Segment])
  def extractIntervals(dataset: List[Segment]): List[Interval] = {
    val x_order = getX_order(dataset)
    val counter = new ListBuffer[Long]
    val segments = new ListBuffer[(Int, Segment)]
    val intervals = new ListBuffer[Double]
    intervals.append(x_order.firstKey().x)
    var nextBoundary = false
    var intervalId = 0
    while (!x_order.isEmpty){
      val entry = x_order.pollFirstEntry()
      val endpoint = entry.getValue
      val segment = endpoint.segment
      if(endpoint.isStart){ segments.append( (intervalId, segment) ) }
      val id = segment.id
      if (counter.contains(id)) {
        counter.remove(counter.indexOf(id))
      } else {
        counter.append(entry.getValue.segment.id)
      }
      if( counter.size == 0 ){ intervalId = intervalId + 1 }

      if (counter.size == 0 || (counter.size == 1 && nextBoundary)) {
        nextBoundary = true
        val x = entry.getKey.x
        intervals.append(x)
      } else {
        nextBoundary = false
      }
    }

    save("/tmp/edgesSBI.wkt"){
      segments.toList.map{ case(i, seg) =>
        s"${seg.wkt}\t${i}\n"
      }
    }

    val ranges = intervals.zip(intervals.tail).zipWithIndex.filter{ case(r, i) => i % 2 == 0 }.map(_._1)
    val subset_prime = segments.toList.groupBy(_._1).values.map(_.map(_._2))
    val subset = subset_prime.toList.sortBy(_.head.source.x)
    val I = ranges.zip(subset).map{ case(r, ss) => Interval(Range(r._1, r._2), ss) }.toList

    I.map{ interval =>
      s"${interval.range}\t${interval.subset.size}"
    }.foreach{println}

    I
  }

  def saveSegments(dataset: List[Segment], filename: String): Unit = {
    save(filename){
      dataset.map{ s =>
        s"${s.wkt}\n"
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val debug: Boolean = true
    val tolerance: Double = 1e-3
    implicit val model: PrecisionModel = new PrecisionModel(1000.0)
    implicit val geofactory: GeometryFactory = new GeometryFactory(model)

    implicit val settings: Settings = Settings(
      debug = debug,
      tolerance = tolerance,
      geofactory = geofactory
    )

    // Reading data...
    val big_dataset   = readSegments(filename = "/home/and/RIDIR/tmp/edgesBDL.wkt")
    val small_dataset = readSegments(filename = "/home/and/RIDIR/tmp/edgesSDL.wkt")

    // Extracting intervals...
    val intervals: List[Interval] = extractIntervals(small_dataset)

    // Indexing the big dataset...
    val subsets1 = getSubsetsByIndex(big_dataset, intervals)

    // Save segments...
    subsets1.zipWithIndex.foreach{ case(subset, i) =>
      save(s"/tmp/edgesSS1${i}.wkt"){
        subset.map{ segment =>
          val wkt = segment.wkt
          s"$wkt\n"
        }
      }
    }

    // Sweeping the big dataset...
    val subsets2 = getSubsetsBySweep2(big_dataset, intervals)

    // Save segments...
    subsets2.zipWithIndex.foreach { case (subset, i) =>
      save(s"/tmp/edgesSS2${i}.wkt") {
        subset.map { segment =>
          val wkt = segment.wkt
          s"$wkt\n"
        }
      }
    }

    // Testing
    // Testing

    subsets1.zip(intervals).zipWithIndex.foreach{ case(z, i) =>
      val bd = z._1
      val sd = z._2.subset
      val intersections = BentleyOttmann.getIntersectionPoints(bd, sd)
      save(s"/tmp/edgesAINT${i}.wkt") {
        intersections.zipWithIndex.map{ case(intersection, j) =>
          val x = intersection.x
          val y = intersection.y
          s"POINT($x $y)\n"
        }.sorted
      }
    }

    subsets2.zip(intervals).zipWithIndex.foreach { case (z, i) =>
      val bd = z._1
      val sd = z._2.subset
      val intersections = BentleyOttmann.getIntersectionPoints(bd, sd)
      save(s"/tmp/edgesBINT${i}.wkt") {
        intersections.zipWithIndex.map { case (intersection, j) =>
          val x = intersection.x
          val y = intersection.y
          s"POINT($x $y)\n"
        }.sorted
      }
    }
  }
}
