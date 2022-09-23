package edu.ucr.dblab.sweeptest

import com.vividsolutions.jts.geom._
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
import YStructure_Tester4._

import scala.collection.immutable.Map

object YStructure_Tester5 extends AnyFlatSpec with should.Matchers {

  def main(args: Array[String]): Unit = {

    def Y_structure_add(s: Segment)(implicit Y: TreeMap[Segment, Segment], cmp: sweep_cmp): Unit = {
      cmp.setSweep(s.source)
      Y.put(s, s)
    }

    def Y_structure_del(s: Segment)(implicit Y: TreeMap[Segment, Segment], cmp: sweep_cmp): Unit = {
      cmp.setSweep(s.source)
      Y.remove(s)
    }

    def Y_structure_content(implicit Y: TreeMap[Segment, Segment]): String = {
      Y.asScala.iterator.filter { case (s, i) => s.id >= 0 }.map { case (s, i) => s.id }.mkString(" ")
    }

    def Y_structure_content2(implicit Y: TreeMap[Long, Segment]): String = {
      Y.asScala.iterator.filter { case (s, i) => s >= 0 }.map { case (s, i) => s }.mkString(" ")
    }

    def readIntervals(filename: String): List[Double] = {
      import scala.io.Source
      val buffer = Source.fromFile(filename)
      val intervals = buffer.getLines().map(_.toDouble).toList
      buffer.close()
      intervals
    }

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
      val bd = generateSegments(n = 100, xRange = 1000.0, yRange = 250.0, label = "A")
      save("/tmp/edgesBD.wkt") {
        bd.map { seg =>
          val wkt = seg.wkt
          val id = seg.id
          s"$wkt\t$id\n"
        }
      }
      bd
    } else {
      readSegments(filename = "/home/and/RIDIR/tmp/edgesBD.wkt")
    }

    val intervals = readIntervals(filename = "/home/and/RIDIR/tmp/intervals.txt")

    // Feeding X_structure...
    case class EndPoint(segment: Segment, isStart: Boolean)
    implicit val X_structure: TreeMap[Coordinate, EndPoint] = new TreeMap[Coordinate, EndPoint]()
    big_dataset.foreach{ segment =>
      val start = EndPoint(segment,  true)
      val end   = EndPoint(segment, false)
      X_structure.put(segment.source, start)
      X_structure.put(segment.target,   end)
    }
    save(filename = "/tmp/edgesXS.wkt") {
      X_structure.asScala.iterator.zipWithIndex.map { case (entry, i) =>
        val point = geofactory.createPoint(entry._1)
        val endpoint = entry._2
        val position = if (endpoint.isStart) "Start" else "End"
        val sid = endpoint.segment.id
        s"${point.toText}\t$i\t${sid}_${position}\n"
      }.toList
    }

    val (lower_sentinel, upper_sentinel) = BentleyOttmann.getSentinels(big_dataset)
    implicit val cmp = new sweep_cmp()
    cmp.setSweep(lower_sentinel.source)
    //implicit val Y_structure: TreeMap[Segment, Segment] = new TreeMap[Segment, Segment](cmp)
    implicit val Y_structure: TreeMap[Long, Segment] = new TreeMap[Long, Segment]()
    //Y_structure_add(upper_sentinel)
    //Y_structure_add(lower_sentinel)

    var n = 1
    while(!X_structure.isEmpty){
      val entry = X_structure.pollFirstEntry()
      val sweep_point = entry.getKey
      val endpoint = entry.getValue

      if(endpoint.isStart){
        //Y_structure_add(endpoint.segment)
        Y_structure.put(endpoint.segment.id, endpoint.segment)
      } else {
        if(endpoint.segment.id == 59){
          println("")
        }
        //Y_structure_del(endpoint.segment)
        Y_structure.remove(endpoint.segment.id)
      }
      val content = Y_structure_content2
      println{ s"${n}. ${sweep_point.x}\t$content" }
      n = n + 1
    }
  }
}
