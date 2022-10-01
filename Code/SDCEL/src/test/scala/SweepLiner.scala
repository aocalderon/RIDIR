package edu.ucr.dblab.sweeptest

import com.vividsolutions.jts.geom.{Coordinate, Envelope, GeometryFactory, LineString, PrecisionModel}
import edu.ucr.dblab.sdcel.geometries.Half_edge
import sdcel.bo.Segment
import edu.ucr.dblab.sdcel.Utils.save

import java.util.Comparator
import java.util.TreeMap
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.Random

object SweepLiner {
  private def mkSegment(p1: (Double, Double), p2: (Double, Double), id: Long)
                       (implicit geofactory: GeometryFactory): Segment = {
    val c1 = new Coordinate(p1._1, p1._2)
    val c2 = new Coordinate(p2._1, p2._2)
    val line = geofactory.createLineString(Array(c1, c2))
    val h = Half_edge(line); h.id = id
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
  private def generateDataset(implicit geofactory: GeometryFactory): List[Segment] = {
    val a = (0 until 10).map{ i =>
      Random.nextDouble() * 100.0
    }
    val b = (0 until 10).map { i =>
      Random.nextDouble() * 100.0
    }
    a.zip(b).zipWithIndex.map{ case(y, i) =>
      val y1 = y._1
      val y2 = y._2
      val p1 = new Coordinate(0, y1)
      val p2 = new Coordinate(100, y2)
      val line = geofactory.createLineString(Array(p1, p2))
      val h = Half_edge(line); h.id = i
      Segment(h, "A")
    }.toList
  }

  private def getStatusOrder(implicit status: TreeMap[Segment, Segment]): String = {
    status.asScala.iterator.map { _._1.id }.mkString(" ")
  }

  def main(args: Array[String]): Unit = {
    implicit val model = new PrecisionModel(1000.0)
    implicit val geofactory = new GeometryFactory(model)

    val dataset = generateDataset
    saveSegments(dataset, "/tmp/edgesSS.wkt")
    val envelope = getEnvelope(dataset)
    val sweepComparator = new SweepComparator(envelope, geofactory)
    implicit val status: TreeMap[Segment, Segment] = new TreeMap[Segment, Segment](sweepComparator)

    val R = (1 to 1000).map { i =>
      val status1 = new ListBuffer[String]
      (0.0 to 100.0 by 10.0).foreach { x =>
        status.clear()
        sweepComparator.setSweep(x)
        status.putAll(dataset.map { s => s -> s }.toMap.asJava)
        status1.append(getStatusOrder)
      }

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

      status1.zip(status2).map { case (a, b) => a == b }.reduce(_ && _)
    }.reduce(_ && _)
    println(R)
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
