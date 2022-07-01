package edu.ucr.dblab.bo3

import scala.collection.mutable.{PriorityQueue}
import scala.collection.JavaConverters._

import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory, Coordinate, Point}
import com.vividsolutions.jts.geomgraph.index.SimpleMCSweepLineIntersector
import com.vividsolutions.jts.geomgraph.index.SegmentIntersector
import com.vividsolutions.jts.algorithm.RobustLineIntersector
import com.vividsolutions.jts.geomgraph.EdgeIntersection
import com.vividsolutions.jts.geomgraph.Edge

import edu.ucr.dblab.debug.BO.{generateRandomHedges, generateFromFile}
import edu.ucr.dblab.sdcel.geometries.{Half_edge, HEdge}
import edu.ucr.dblab.sdcel.Utils.{save, logger}

object BO3 {
  def main(args: Array[String]): Unit = {
    val params = new BOConf(args)

    val debug: Boolean    = params.debug()
    val method: String    = params.method()
    val filename: String  = params.file1()
    val n: Int            = params.n()
    val runId: Int        = params.runid()
    val tolerance: Double = params.tolerance()
    val scale: Double     = 1 / tolerance

    implicit val model: PrecisionModel = new PrecisionModel(scale)
    implicit val geofactory: GeometryFactory = new GeometryFactory(model)

    implicit val settings: Settings = Settings(
      debug = debug,
      tolerance = tolerance,

      geofactory = geofactory
    )


    println(s"METHOD:     $method   ")
    println(s"TOLERANCE:  $tolerance")
    println(s"SCALE:      ${geofactory.getPrecisionModel.getScale}")
    println(s"N:          $n        ")
    println(s"FILE:       $filename ")
    println(s"DEBUG:      $debug    ")

    ////////////////////////////////////////////////////////////////
    import java.util.{PriorityQueue, TreeSet}
    import scala.collection.mutable.ArrayBuffer
    import java.io.PrintWriter

    val p0  = new Coordinate(2, 1); val p1  = new Coordinate(7, 4)
    val p2  = new Coordinate(7, 1); val p3  = new Coordinate(1, 4)
    val p4  = new Coordinate(3, 4); val p5  = new Coordinate(5, 6)
    val p6  = new Coordinate(5, 4); val p7  = new Coordinate(3, 6)
    val p8  = new Coordinate(2, 5); val p9  = new Coordinate(7, 8)
    val p10 = new Coordinate(2, 8); val p11 = new Coordinate(5, 8)
    val l1 = geofactory.createLineString(Array(p0,  p1))
    val l2 = geofactory.createLineString(Array(p2,  p3))
    val l3 = geofactory.createLineString(Array(p4,  p5))
    val l4 = geofactory.createLineString(Array(p6,  p7))
    val l5 = geofactory.createLineString(Array(p8,  p9))
    val l6 = geofactory.createLineString(Array(p10, p11))
    val h1 = Half_edge(l1); h1.id = 1
    val h2 = Half_edge(l2); h2.id = 2
    val h3 = Half_edge(l3); h3.id = 3
    val h4 = Half_edge(l4); h4.id = 4
    val h5 = Half_edge(l5); h5.id = 5
    val h6 = Half_edge(l6); h6.id = 6
    val hh1 = List(h1, h3, h5)
    val hh2 = List(h2, h4, h6)
    val segs1 = hh1.map{ h => Segment(h, "A") }
    val segs2 = hh2.map{ h => Segment(h, "B") }
    if(debug){
      save("/tmp/edgesSS1.wkt"){
        segs1.map( e => s"${e.wkt}\t${e.lid}\n" )
      }
      save("/tmp/edgesSS2.wkt"){
        segs2.map( e => s"${e.wkt}\t${e.lid}\n" )
      }
    }
    val segs = segs1 ++ segs2
    implicit var status: TreeSet[Node] = new TreeSet[Node]()

    implicit var scheduler: PriorityQueue[Event] = new PriorityQueue[Event]()
    segs.foreach { seg =>
      scheduler.add( Event( seg.first,  List(seg), 0 ) )
      scheduler.add( Event( seg.second, List(seg), 1 ) )
    }
    val s4 = segs.filter(_.id == 4).head
    val s5 = segs.filter(_.id == 5).head
    scheduler.add( Event( new Coordinate(4.27, 2.36), List(s4, s5), 2 ) )
    val s3 = segs.filter(_.id == 3).head
    scheduler.add( Event( new Coordinate(4, 5), List(s3, s4), 2 ) )
    val s1 = segs.filter(_.id == 1).head
    val s2 = segs.filter(_.id == 2).head
    scheduler.add( Event( new Coordinate(3.25, 5.75), List(s1, s2), 2 ) )

    if(debug){
      save("/tmp/edgesSCH.wkt"){
        scheduler.iterator.asScala.toList.map( e => s"${e.wkt(maxY = 10)}\t${e.value}\t${e.ttype}\n" )
      }
    }

    // Load data...
    val (points, data) = BentleyOttmann.loadData
    // Calling main function...
    BentleyOttmann.sweep_segments( data, List.empty[Segment] )

    if(debug){
      save("/tmp/edgesPts.wkt"){
        points.map(p => s"${p.toText}\t${p.getUserData}\n")
      }
      save("/tmp/edgesSegs.wkt"){
        data.map(s => s"${s.wkt}\n")
      }
    }

    def printStatus(f: PrintWriter, x: Double, filter: String = "*")
      (implicit status: TreeSet[Node]) = {

      println("Printing Status...")
      filter match {
        case "*" => status.asScala.toList.zipWithIndex
            .map{ case(s, x)  => s"$x\t$s\tSTATUS" }.foreach{ println }
        case _   => status.iterator().asScala.filter(_.segments.head.label != filter).zipWithIndex
            .map{ case(s, x)  => s"$s\t$x" }.foreach{ println }
      }
      println("Printing Status... Done.")
      val wkt = status.asScala.zipWithIndex.map{ case(node, id) =>
        s"POINT (${x} ${node.value})\t${id}\n"
      }.mkString("")
      f.write(wkt)
    }
  }

  def sweepline(hs1: List[Half_edge], hs2: List[Half_edge], debug: Boolean = false)
    (implicit geofactory: GeometryFactory): List[Intersection] = {

    val edges1 = hs1.map{ h => Segment(h, "A") }
    val edges2 = hs2.map{ h => Segment(h, "B") }
    if(debug){
      save("/tmp/edgesS1.wkt"){
        edges1.map( e => s"${e.wkt}\n" )
      }
      save("/tmp/edgesS2.wkt"){
        edges2.map( e => s"${e.wkt}\n" )
      }
    }
    val edges = edges1 ++ edges2

    val intersector = BentleyOttmann
    intersector.readSegments( edges )
    val intersections = intersector.getIntersections

    if(debug){
      save("/tmp/edgesI1.wkt"){
        intersections.zipWithIndex.map{ case (intersect, id)  =>
          val i = geofactory.createPoint(intersect.p)
          i.setUserData(id)
        
          val wkt = i.toText
          s"$wkt\t$id\n"
        }
      }
    }

    intersections.toList
  }

  def sweeplineJTS(hs1: List[Half_edge], hs2: List[Half_edge], debug: Boolean = false)
    (implicit geofactory: GeometryFactory): List[Point] = {

    val aList = hs1.map{ h =>
      val pts = Array(h.v1, h.v2)
      HEdge(pts, h)
    }.asJava
    
    val bList = hs2.map{ h =>
      val pts = Array(h.v1, h.v2)
      HEdge(pts, h)
    }.asJava

    val sweepline = new SimpleMCSweepLineIntersector()
    val lineIntersector = new RobustLineIntersector()
    val segmentIntersector = new SegmentIntersector(lineIntersector, true, true)
    sweepline.computeIntersections(aList, bList, segmentIntersector)

    val aPoints = aList.asScala.flatMap{ a =>
      val iList = a.getEdgeIntersectionList.iterator.asScala.toList
      if(iList.size == 0){
        List.empty[Point]
      } else {
        iList.map{ i =>
          val coord = i.asInstanceOf[EdgeIntersection].getCoordinate
          geofactory.createPoint(coord)
        }.toList
      }
    }.toList.distinct

    val bPoints = bList.asScala.flatMap{ a =>
      val iList = a.getEdgeIntersectionList.iterator.asScala.toList
      if(iList.size == 0){
        List.empty[Point]
      } else {
        iList.map{ i =>
          val coord = i.asInstanceOf[EdgeIntersection].getCoordinate
          geofactory.createPoint(coord)
        }.toList
      }
    }.toList.distinct

    val ab = (aPoints ++ bPoints).distinct

    if(debug){
      save("/tmp/edgesI2.wkt"){
        ab.map{ p =>
          s"${p}\n"
        }
      }
    }

    ab
  }  
}

import org.rogach.scallop._

class BOConf(args: Seq[String]) extends ScallopConf(args) {
  val debug: ScallopOption[Boolean]    = opt[Boolean] (default = Some(true))
  val method: ScallopOption[String]    = opt[String]  (default = Some("Random"))
  val file1: ScallopOption[String]     = opt[String]  (default = Some("")) 
  val file2: ScallopOption[String]     = opt[String]  (default = Some("")) 
  val tolerance: ScallopOption[Double] = opt[Double]  (default = Some(1e-3))
  val n: ScallopOption[Int]            = opt[Int]     (default = Some(250))
  val runid: ScallopOption[Int]        = opt[Int]     (default = Some(0))

  val embed: ScallopOption[Boolean]        = opt[Boolean] (default = Some(false))
  val optimization: ScallopOption[Boolean] = opt[Boolean] (default = Some(false))  

  verify()
}

