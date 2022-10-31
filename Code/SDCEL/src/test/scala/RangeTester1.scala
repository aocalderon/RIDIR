package edu.ucr.dblab.sweeptest

import com.vividsolutions.jts.geom._
import com.vividsolutions.jts.index.strtree.STRtree
import edu.ucr.dblab.sdcel.Utils.{envelope2WKT, save}
import edu.ucr.dblab.sdcel.geometries.Half_edge
import org.scalatest.flatspec._
import org.scalatest.matchers._
import sdcel.bo._

import java.util.TreeMap
import scala.collection.JavaConverters._
import YStructure_Tester4.{EndPoint, getX_orderBySegment, readSegments}
import edu.ucr.dblab.sweeptest.RangeTester1.Interval

import java.util
import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

object RangeTester1 extends AnyFlatSpec with should.Matchers {

  def getEnvelope(segments: List[Segment]): Envelope = {
      segments.map(_.envelope).reduce{ (a,b) =>
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
      val a = EndPoint(segment.source, segment, true)
      val b = EndPoint(segment.target, segment, false)
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
    }.groupBy(_.source).foreach{ case(point, list) =>
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

  /*
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
*/
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
          if(current.r >= head.source.x){
            val subset_  = subset :+ head
            val current_ = if(current.r >= head.target.x) current else Range(current.l, head.target.x)
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

  def extractIntervals(dataset: List[Segment]): List[Interval] = {
    val I = extractIntervals2(dataset)

    I.zipWithIndex.foreach{ case(interval, i) =>
      saveSegments(interval.subset, s"/tmp/edgesSDS${i}.wkt")
    }

    I
  }

  def saveSegments(dataset: List[Segment], filename: String): Unit = {
    save(filename){
      dataset.map{ s =>
        s"${s.wkt}\n"
      }
    }
  }

  private def readGeoms(filename: String, origin: Coordinate)(implicit geofactory: GeometryFactory): List[(Geometry, Double, Int)] = {
    import scala.io.Source
    import com.vividsolutions.jts.io.WKTReader
    val reader = new WKTReader(geofactory)
    val buffer = Source.fromFile(filename)
    val geoms = buffer.getLines().map { line =>
      val arr = line.split("\t")
      val wkt = arr(0)
      reader.read(wkt)
    }.filter(geom => geom.isInstanceOf[Polygon] || geom.isInstanceOf[MultiPolygon])
      .map { geom =>
        val poly = geom.getGeometryType match {
          case "Polygon" => geom.asInstanceOf[Polygon].getExteriorRing
          case _ => geom.getGeometryN(0).asInstanceOf[Polygon].getExteriorRing
        }

        (poly, geom.getCentroid.getCoordinate.distance(origin), poly.getNumPoints)
      }.toList
    buffer.close()

    var sum = 0
    val list = new ListBuffer[Int]()
    val geoms_iter = geoms.sortBy(_._2).toIterator
    while(!geoms_iter.isEmpty){
      val g = geoms_iter.next()
      sum = sum + g._3
      list.append(sum)
    }
    val geoms_prime = geoms.sortBy(_._2).zip(list.toList).map{ case(g, s) =>
      (g._1, g._2, s)
    }
    geoms_prime
  }
  def generateDatasetsFromWKT(A: String, B: String, origin: Coordinate, sample_distance: Int, increase_distance: Int,
                              n: Int, path: String)(implicit geofactory: GeometryFactory): Unit = {

    val geomsA = readGeoms(A, origin)
    val geomsB = readGeoms(B, origin)

    val sample = geomsA.filter(_._3 <= sample_distance).map(_._1)
    val sd = getSegmentsFromGeometry(sample, "A")
    saveSegments(sd, path + "/A.wkt")

    (increase_distance to (increase_distance * n) by increase_distance).foreach{ x =>
      val sample = geomsB.filter(_._3 <= x).map(_._1)
      val bd = getSegmentsFromGeometry(sample, "B")
      saveSegments(bd, path + s"/B${x}.wkt")
    }
  }

  private def getSegmentsFromGeometry(geoms: List[Geometry], lab: String = "")(implicit geofactory: GeometryFactory): List[Segment] = {
    geoms.flatMap { geom =>
      val coordinates = geom.getCoordinates
      coordinates.zip(coordinates.tail).zipWithIndex.map { case (points, id) =>
        val p1 = points._1
        val p2 = points._2
        val l = geofactory.createLineString(Array(p1, p2))
        val h = Half_edge(l)
        h.id = id
        Segment(h, lab)
      }
    }
  }
  def readSegmentsFromPolygons(filename: String)(implicit geofactory: GeometryFactory): List[Segment] = {
    import scala.io.Source
    import com.vividsolutions.jts.io.WKTReader
    val reader = new WKTReader(geofactory)
    val buffer = Source.fromFile(filename)
    val segs = buffer.getLines().flatMap { line =>
      val arr = line.split("\t")
      val wkt = arr(0)
      val lab = arr(1).substring(0, 1)
      val poly = reader.read(wkt)
      val coordinates = poly.getCoordinates
      coordinates.zip(coordinates.tail).zipWithIndex.map{ case(points, id) =>
        val p1 = points._1
        val p2 = points._2
        val l = geofactory.createLineString(Array(p1,p2))
        val h = Half_edge(l)
        h.id = id
        Segment(h, lab)
      }
    }.toList
    buffer.close()
    segs
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

    val A = "/home/and/Datasets/BiasIntersections/PA/PA1.wkt"
    val B = "/home/and/Datasets/BiasIntersections/PA/PA2.wkt"
    val origin = new Coordinate(3686670, 1103563)
    val sample_distance = 3200
    val increase_distance = 3200
    val n = 7
    val path = "/home/and/RIDIR/Datasets/BiasIntersections/PA3"
    generateDatasetsFromWKT(A, B, origin, sample_distance, increase_distance, n, path)

    // Reading data...
    //val f1 = "/home/and/RIDIR/tmp/edgesBDL.wkt"
    val f1 = "/home/and/RIDIR/Datasets/BiasIntersections/PA2/B25K.wkt"
    //val f2 = "/home/and/RIDIR/tmp/edgesSDL.wkt"
    val f2 = "/home/and/RIDIR/Datasets/BiasIntersections/PA2/A7K.wkt"
    val big_dataset   = readSegmentsFromPolygons(filename = f1)
    val small_dataset = readSegmentsFromPolygons(filename = f2)

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
    subsets1.zip(intervals).zipWithIndex.foreach{ case(z, i) =>
      val bd = z._1
      val sd = z._2.subset
      val intersections = BentleyOttmann.getIntersectionPoints2(bd, sd)
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
      val intersections = BentleyOttmann.getIntersectionPoints2(bd, sd)
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
