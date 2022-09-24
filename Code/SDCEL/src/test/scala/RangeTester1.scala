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
import YStructure_Tester5.readIntervals
import com.vividsolutions.jts.geom
import com.vividsolutions.jts.index.strtree

import scala.collection.mutable.ListBuffer

object RangeTester1 extends AnyFlatSpec with should.Matchers {

  def getEnvelope(segments: List[Segment]): Envelope = {
      segments.map(_.envelope).reduce{ (a,b) =>
        a.expandToInclude(b)
        a
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
    val big_dataset = readSegments(filename = "/home/and/RIDIR/tmp/edgesBD.wkt")
    val intervals  = readIntervals(filename = "/home/and/RIDIR/tmp/intervals2.txt")

    // Feeding index...
    val index = new STRtree()
    big_dataset.foreach{ segment =>
      index.insert(segment.envelope, segment)
    }

    // Querying index
    val big_envelope = getEnvelope(big_dataset)
    val miny = big_envelope.getMinY
    val maxy = big_envelope.getMaxY
    val subsets1 = intervals.map{ range =>
      val minx = range._1
      val maxx = range._2
      val envelope = new Envelope(minx, maxx, miny, maxy)
      println(envelope2WKT(envelope))
      index.query(envelope).asScala.map(_.asInstanceOf[Segment]).toList
    }

    // Save segments
    subsets1.zipWithIndex.foreach{ case(subset, i) =>
      save(s"/tmp/edgesSS${i}.wkt"){
        subset.map{ segment =>
          val wkt = segment.wkt
          s"$wkt\n"
        }
      }
    }

    val x_order = getX_orderBySegment(big_dataset)
    val y_order: TreeMap[Long, Segment] = new TreeMap[Long, Segment]()
    val xl = 875.0
    val xr = 925.0
    var endpoint: EndPoint = null
    do{
      endpoint = x_order.pollFirstEntry().getValue
      if(endpoint.point.x <= xl){
        if (endpoint.isStart) {
          y_order.put(endpoint.segment.id, endpoint.segment)
        } else {
          y_order.remove(endpoint.segment.id)
        }
      }
    }while(endpoint.point.x <= xl)
    do {
      endpoint = x_order.pollFirstEntry().getValue
      if (endpoint.isStart && endpoint.point.x <= xr) {
        y_order.put(endpoint.segment.id, endpoint.segment)
      } else {
      }
    }while (endpoint.point.x <= xr)

    save("/tmp/edgesSR.wkt") {
      y_order.asScala.iterator.map(_._2.wkt + "\n").toList
    }

    val subset1 = subsets1(1)
    val subset2 = y_order.asScala.iterator.map(_._2).toList

    println(s"${subset1.size} vs ${subset2.size}")

    save("/tmp/edgesSS1.wkt"){
      subset1.map{ segment =>
        segment.wkt + "\n"
      }.sorted
    }
    save("/tmp/edgesSS2.wkt") {
      subset2.map { segment =>
        segment.wkt + "\n"
      }.sorted
    }
  }
}
