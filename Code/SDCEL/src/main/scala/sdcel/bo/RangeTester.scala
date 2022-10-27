package sdcel.bo

import com.vividsolutions.jts.geom.{Coordinate, Envelope, GeometryFactory, PrecisionModel}
import com.vividsolutions.jts.index.strtree.STRtree
import edu.ucr.dblab.sdcel.Utils.save
import edu.ucr.dblab.sdcel.geometries.Half_edge

import java.util
import scala.annotation.tailrec
import scala.collection.JavaConverters._

object RangeTester {

  def getEnvelope(segments: List[Segment]): Envelope = {
    segments.map(_.envelope).reduce { (a, b) =>
      a.expandToInclude(b)
      a
    }
  }

  case class EndPoint(point: Coordinate, segment: Segment, isStart: Boolean)

  def getX_order(segments: List[Segment]): util.TreeMap[Coordinate, List[EndPoint]] = {
    val x_order = new util.TreeMap[Coordinate, List[EndPoint]]()
    val endpoints = segments.flatMap { segment_prime =>
      // Segments need to be left or upwards oriented...
      val segment = if (!segment_prime.isVertical) {
        if (segment_prime.isLeftOriented) {
          segment_prime
        } else {
          segment_prime.reverse
        }
      } else {
        if (segment_prime.isUpwardsOriented) {
          segment_prime
        } else {
          segment_prime.reverse
        }
      }
      val a = EndPoint(segment.source, segment, isStart = true)
      val b = EndPoint(segment.target, segment, isStart = false)
      List(a, b)
    }.groupBy(_.point)
    endpoints.foreach { endpoints =>
      val point = endpoints._1
      x_order.put(point, endpoints._2)
    }
    x_order
  }

  def getX_orderLeftOriented(segments: List[Segment]): List[Segment] = {
    segments.map { segment_prime =>
      // Segments need to be left or upwards oriented...
      if (!segment_prime.isVertical) {
        if (segment_prime.isLeftOriented) {
          segment_prime
        } else {
          segment_prime.reverse
        }
      } else {
        if (segment_prime.isUpwardsOriented) {
          segment_prime
        } else {
          segment_prime.reverse
        }
      }
    }.sortBy(_.source.x)
  }

  def getX_orderBySegment(segments: List[Segment]): util.TreeMap[Coordinate, List[Segment]] = {
    val x_order = new util.TreeMap[Coordinate, List[Segment]]()
    segments.map { segment_prime =>
      // Segments need to be left or upwards oriented...
      if (!segment_prime.isVertical) {
        if (segment_prime.isLeftOriented) {
          segment_prime
        } else {
          segment_prime.reverse
        }
      } else {
        if (segment_prime.isUpwardsOriented) {
          segment_prime
        } else {
          segment_prime.reverse
        }
      }
    }.groupBy(_.source).foreach { case (point, list) =>
      x_order.put(point, list)
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
        .asScala.values.toList.flatten

      // Adding to the final result...
      subset_before ++ subset_after
    }

    subsets
  }

  case class Range(l: Double, r: Double)

  case class Interval(range: Range, subset: List[Segment])

  def extractIntervals2(dataset: List[Segment]): List[Interval] = {
    val x_order = getX_orderLeftOriented(dataset)

    @tailrec
    def getIntervals(current: Range, subset: List[Segment], segments: List[Segment], r: List[Interval]): List[Interval] = {
      segments match {
        case Nil => r :+ Interval(current, subset)
        case head :: tail =>
          if (current.r >= head.source.x) {
            val subset_ = subset :+ head
            val current_ = if (current.r >= head.target.x) current else Range(current.l, head.target.x)
            getIntervals(current_, subset_, tail, r)
          } else {
            val r_ = r :+ Interval(current, subset)
            val current_ = Range(head.source.x, head.target.x)
            getIntervals(current_, List(head), tail, r_)
          }
      }
    }

    val first_segment = x_order.head
    val first_range = Range(first_segment.source.x, first_segment.target.x)

    getIntervals(first_range, List(first_segment), x_order.tail, List.empty[Interval])
  }

  def extractIntervals(dataset: List[Segment])(implicit settings: Settings): List[Interval] = {
    val I = extractIntervals2(dataset)

    if (settings.debug) {
      I.zipWithIndex.foreach { case (interval, i) =>
        saveSegments(interval.subset, s"/tmp/edgesSDS$i.wkt")
      }
    }

    I
  }

  def saveSegments(dataset: List[Segment], filename: String): Unit = {
    save(filename) {
      dataset.map { s =>
        s"${s.wkt}\n"
      }
    }
  }

  def readSegmentsFromPolygons(filename: String)(implicit geofactory: GeometryFactory): List[Segment] = {
    import com.vividsolutions.jts.io.WKTReader

    import scala.io.Source
    val reader = new WKTReader(geofactory)
    val buffer = Source.fromFile(filename)
    val segs = buffer.getLines().flatMap { line =>
      val arr = line.split("\t")
      val wkt = arr(0)
      val lab = arr(1).substring(0, 1)
      val poly = reader.read(wkt)
      val coordinates = poly.getCoordinates
      coordinates.zip(coordinates.tail).zipWithIndex.map { case (points, id) =>
        val p1 = points._1
        val p2 = points._2
        val l = geofactory.createLineString(Array(p1, p2))
        val h = Half_edge(l)
        h.id = id
        Segment(h, lab)
      }
    }.toList
    buffer.close()
    segs
  }

  def log(msg: String)(implicit settings: Settings): Unit = {
    val now = System.currentTimeMillis
    println(s"$now\t${settings.appId}\t${settings.tag1}\t${settings.tag2}\t$msg")
  }

  def main(args: Array[String]): Unit = {
    val params = new RangerParams(args)
    implicit val model: PrecisionModel = new PrecisionModel(1000.0)
    implicit val geofactory: GeometryFactory = new GeometryFactory(model)

    implicit val settings: Settings = Settings(
      debug = params.debug(),
      tolerance = params.tolerance(),
      tag1 = params.tag1(),
      tag2 = params.tag2(),
      geofactory = geofactory
    )

    // Reading data...
    val f1 = params.input1()
    val f2 = params.input2()
    val big_dataset   = readSegmentsFromPolygons(filename = f1)
    val small_dataset = readSegmentsFromPolygons(filename = f2)

    // Running traditional method...
    log(s"Traditional\tSTART")
    val bd = big_dataset ++ small_dataset
    val I1 = BentleyOttmann.getIntersectionPoints1(bd)
    log(s"Traditional\tEND")

    // Running sweeping method...
    log("Sweeping\tSTART")
    val intervals: List[Interval] = extractIntervals(small_dataset)
    val subsets = getSubsetsBySweep2(big_dataset, intervals)
    val I2 = subsets.zip(intervals).map { case (bd, interval) =>
      val sd = interval.subset
      BentleyOttmann.getIntersectionPoints2(bd, sd)
    }.reduce {
      _ ++ _
    }
    log("Sweeping\tEND")

    if (settings.debug) {
      save(s"/tmp/edgesINT1.wkt") {
        I1.map { intersection =>
          val x = intersection.x
          val y = intersection.y
          s"POINT($x $y)\n"
        }.sorted
      }
      save(s"/tmp/edgesINT2.wkt") {
        I2.map { intersection =>
          val x = intersection.x
          val y = intersection.y
          s"POINT($x $y)\n"
        }.sorted
      }
    }
  }
}

import org.rogach.scallop._
class RangerParams(args: Seq[String]) extends ScallopConf(args) {
  val tolerance:   ScallopOption[Double]  = opt[Double]  (default = Some(0.001))
  val input1:      ScallopOption[String]  = opt[String]  (default = Some("/home/and/RIDIR/Datasets/BiasIntersections/PA2/B50000.wkt"))
  val input2:      ScallopOption[String]  = opt[String]  (default = Some("/home/and/RIDIR/Datasets/BiasIntersections/PA2/A.wkt"))
  val tag1:        ScallopOption[String]  = opt[String]  (default = Some("25K"))
  val tag2:        ScallopOption[String]  = opt[String]  (default = Some("7K"))
  val appId:       ScallopOption[Int]     = opt[Int]     (default = Some(0))
  val debug:       ScallopOption[Boolean] = opt[Boolean] (default = Some(false))

  verify()
}