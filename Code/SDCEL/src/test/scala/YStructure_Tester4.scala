package edu.ucr.dblab.sweeptest

import com.vividsolutions.jts.geom.{Coordinate, Envelope, GeometryFactory, LineString, PrecisionModel}
import com.vividsolutions.jts.io.WKTReader
import edu.ucr.dblab.sdcel.Utils.save
import edu.ucr.dblab.sdcel.geometries.Half_edge
import org.scalatest.flatspec._
import org.scalatest.matchers._
import sdcel.bo._

import java.util.TreeMap
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.Random

object YStructure_Tester4 extends AnyFlatSpec with should.Matchers {

  def orderPoints(x1: Double, y1: Double, x2: Double, y2: Double): Array[Coordinate] = {
    val (a, b) = if(x1 < x2){ (x1, x2) } else { (x2, x1) }
    val (c, d) = if(y1 < y2){ (y1, y2) } else { (y2, y1) }
    val p = new Coordinate(a, c)
    val q = new Coordinate(b, d)
    Array(p, q)
  }

  def generateSegments(n: Int, yRange: Double = 100.0, xRange: Double = 100.0, label: String = "*", length: Double = 50.0)
                      (implicit geofactory: GeometryFactory): List[Segment] = {
    (0 to n).map{ i =>
      val x1 = Random.nextDouble() * xRange
      val y1 = Random.nextDouble() * yRange
      val x2 = x1 + Random.nextDouble() * length
      val y2 = y1 + Random.nextDouble() * length
      val points = orderPoints(x1,y1,x2,y2)
      val l = geofactory.createLineString(points)
      val h = Half_edge(l); h.id = i
      Segment(h, label)
    }.toList
  }

  def generateSegmentsEnv(env: Envelope, n: Int , label: String = "*")
                      (implicit geofactory: GeometryFactory): List[Segment] = {
    val a = env.getMinX; val b = env.getMinY;
    val c = env.getMaxX; val d = env.getMaxY;
    val xRange = Math.abs(c - a)
    val yRange = Math.abs(b - d)
    (0 to n).map { i =>
      val x1 = a + ( Random.nextDouble() * xRange )
      val y1 = b + ( Random.nextDouble() * yRange )
      val x2 = a + ( Random.nextDouble() * xRange )
      val y2 = b + ( Random.nextDouble() * yRange )
      val points = orderPoints(x1, y1, x2, y2)
      val l = geofactory.createLineString(points)
      val h = Half_edge(l);
      h.id = i
      Segment(h, label)
    }.toList
  }
  def readSegments(filename: String)(implicit geofactory: GeometryFactory): List[Segment] = {
    import scala.io.Source
    val reader = new WKTReader(geofactory)
    val buffer = Source.fromFile(filename)
    val segs = buffer.getLines().map { line =>
      val arr = line.split("\t")
      val wkt = arr(0)
      val lab = arr(1).substring(0,1)
      val id  = arr(1).substring(1).toInt
      val l: LineString = reader.read(wkt).asInstanceOf[LineString]
      val h = Half_edge(l); h.id = id
      Segment(h, lab)
    }.toList
    buffer.close()
    segs
  }

  def extractIntervals(x_order: TreeMap[Coordinate, Long]): List[Double] = {
    val counter = new ListBuffer[Long]
    val intervals = new ListBuffer[Double]
    intervals.append(x_order.firstKey().x)
    var nextBoundary = false
    while (!x_order.isEmpty) {
      val entry = x_order.pollFirstEntry()
      val id = entry.getValue
      if (counter.contains(id)) {
        counter.remove(counter.indexOf(id))
      } else {
        counter.append(entry.getValue)
      }
      val boundary = if (counter.size == 0 || (counter.size == 1 && nextBoundary)) {
        nextBoundary = true
        val x = entry.getKey.x
        intervals.append(x)
        s"LINESTRING( ${x} 0, ${x} 250 )"
      } else {
        nextBoundary = false
        ""
      }
      //println(s"${counter.mkString(" ")}   :${counter.size} ${boundary}")
    }
    intervals.toList
  }

  def createBoundaries(n: Int): String = {
    val boundaries_prime = (1 to n).map { r => Random.nextInt(1000) }.sorted
    val boundaries = boundaries_prime.zip(boundaries_prime.tail).zipWithIndex
      .filter { case (r, i) => i % 2 == 0 }
      .map { case (r, i) =>
        val start = r._1
        val end = r._2
        s"$start $end"
      }.mkString(",")

    boundaries
  }
  def generateSmallDataset(envelope: Envelope, boundaries: String, n: Int = 10, label: String = "B")
                          (implicit geofactory: GeometryFactory): List[Segment] = {
    val y1 = envelope.getMinY
    val y2 = envelope.getMaxY

    boundaries.split(",").zipWithIndex.map { case (interval, i) =>
      val arr = interval.split(" ")
      val x1 = arr(0).toDouble
      val x2 = arr(1).toDouble
      val env = new Envelope(x1, x2, y1, y2)
      generateSegmentsEnv(env, n, s"${label}$i")
    }.reduceLeft { (a, b) => a ++ b }
  }

  def getX_order(small_dataset: List[Segment]): TreeMap[Coordinate, Long] = {
    case class EndPoint(position: Coordinate, id: Long)
    val x_order = new TreeMap[Coordinate, Long]()
    small_dataset.map { segment =>
      val a = EndPoint(segment.source, segment.id)
      val b = EndPoint(segment.target, segment.id)
      List(a, b)
    }.flatten.foreach { endpoint =>
      x_order.put(endpoint.position, endpoint.id)
    }

    save(filename = "/tmp/edgesXO.wkt") {
      x_order.asScala.iterator.zipWithIndex.map { case (endpoint, order) =>
        s"POINT( ${endpoint._1.x} ${endpoint._1.y} )\t${order}\t${endpoint._1.x}\t${endpoint._2}\n"
      }.toList
    }
    x_order
  }

  case class EndPoint(point: Coordinate, segment: Segment, isStart: Boolean)
  def getX_orderBySegment(segments: List[Segment]): TreeMap[Coordinate, EndPoint] = {
    val x_order = new TreeMap[Coordinate, EndPoint]()
    segments.map { segment =>
      val a = EndPoint(segment.source, segment, true)
      val b = EndPoint(segment.target, segment, false)
      List(a, b)
    }.flatten.foreach { endpoint =>
      x_order.put(endpoint.point, endpoint)
    }

    save(filename = "/tmp/edgesXO.wkt") {
      x_order.asScala.iterator.zipWithIndex.map { case (endpoint, order) =>
        s"POINT( ${endpoint._1.x} ${endpoint._1.y} )\t${order}\t${endpoint._1.x}\t${endpoint._2.segment.id}\n"
      }.toList
    }
    x_order
  }

  def getEnvelope(segments: List[Segment]): Envelope = {
    segments.map(_.envelope).reduce { (a, b) =>
      a.expandToInclude(b)
      a
    }
  }

  def getBoundaries(envelope: Envelope, n: Int = 3, width: Int = 10): String = {
    val boundaries_prime = (0 until n).flatMap { i =>
      val l = (Random.nextDouble() * envelope.getMaxX).toInt
      val r = l + (Random.nextDouble() * width).toInt
      List(l, r)
    }.sorted
    val boundaries = boundaries_prime.zip(boundaries_prime.tail).zipWithIndex
      .filter { case (r, i) => i % 2 == 0 }
      .map { case (range, i) =>
        val l = range._1
        val r = range._2
        s"$l $r"
      }.mkString(",")
    println(s"Boundaries: $boundaries")

    boundaries
  }

  def main(args: Array[String]): Unit = {
    val debug: Boolean = true
    val tolerance: Double = 1e-3
    val generate: Boolean = false
    implicit val model: PrecisionModel = new PrecisionModel(1000.0)
    implicit val geofactory: GeometryFactory = new GeometryFactory(model)

    implicit val settings: Settings = Settings(
      debug = debug,
      tolerance = tolerance,
      geofactory = geofactory
    )

    val big_dataset = if (generate) {
      val bd = generateSegments(n = 1500, xRange = 10000.0, yRange = 1000.0, length = 500.0, label = "A")
      save("/tmp/edgesBDL.wkt") {
        bd.map { seg =>
          val wkt = seg.wkt
          val id = seg.id
          s"$wkt\t$id\n"
        }
      }
      bd
    } else {
      readSegments(filename = "/home/and/RIDIR/tmp/edgesBDL.wkt")
    }

    val envelope = getEnvelope(big_dataset)
    val boundaries = getBoundaries(envelope, n = 3, width = 750)
    val sd = generateSmallDataset(envelope, boundaries, n = 50)
    save("/tmp/edgesSDL.wkt") {
      sd.map { seg =>
        val wkt = seg.wkt
        val id = seg.id
        s"$wkt\t$id\n"
      }
    }
  }
}
