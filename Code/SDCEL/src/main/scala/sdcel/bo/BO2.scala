package edu.ucr.dblab.debug

import scala.collection.mutable.{PriorityQueue}
import scala.collection.JavaConverters._

import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory, Coordinate, Point}
import com.vividsolutions.jts.geomgraph.index.SimpleMCSweepLineIntersector
import com.vividsolutions.jts.geomgraph.index.SegmentIntersector
import com.vividsolutions.jts.algorithm.RobustLineIntersector
import com.vividsolutions.jts.geomgraph.EdgeIntersection
import com.vividsolutions.jts.geomgraph.Edge

import edu.ucr.dblab.bo2.{BentleyOttmann2, Segment, Intersection}
import edu.ucr.dblab.debug.BO.{generateRandomHedges, generateFromFile, sweepline}
import edu.ucr.dblab.sdcel.geometries.{Half_edge, HEdge}
import edu.ucr.dblab.sdcel.geometries.{EventPoint, EventPoint_Ordering, CoordYX_Ordering}
import edu.ucr.dblab.sdcel.geometries.{LEFT_ENDPOINT, INTERSECT, RIGHT_ENDPOINT}
import edu.ucr.dblab.sdcel.Utils.{save, logger}

object BO2 {
  def main(args: Array[String]): Unit = {
    val params = new BOConf(args)

    val debug: Boolean    = params.debug()
    val method: String    = params.method()
    val file1: String     = params.file1()
    val file2: String     = params.file2()
    val n: Int            = params.n()
    val runId: Int        = params.runid()
    val tolerance: Double = params.tolerance()
    val scale: Double     = 1 / tolerance

    implicit val model: PrecisionModel = new PrecisionModel(scale)
    implicit val geofactory: GeometryFactory = new GeometryFactory(model)

    println(s"METHOD:     $method   ")
    println(s"TOLERANCE:  $tolerance")
    println(s"SCALE:      ${geofactory.getPrecisionModel.getScale}")
    println(s"N:          $n        ")
    println(s"FILE 1:     $file1    ")
    println(s"FILE 2:     $file2    ")
    println(s"DEBUG:      $debug    ")

    val (hs1, hs2) = method match {
      case "Random" =>
        ( generateRandomHedges(n), generateRandomHedges(n, "B") )
      case "File"   =>
        ( generateFromFile(file1).filter(!_.isVertical).filter(!_.isHorizontal),
          generateFromFile(file2).filter(!_.isVertical).filter(!_.isHorizontal) )
    }

    if(debug){
      save("/tmp/edgesH1.wkt"){
        hs1.map{ h => s"${h.wkt}\t${h.id}\n" }
      }
      save("/tmp/edgesH2.wkt"){
        hs2.map{ h => s"${h.wkt}\t${h.id}\n" }
      }
    }

    val inters1 = sweepline2(hs1, hs2, debug)
    val inters2 = sweeplineJTS2(hs1, hs2, debug)

    val n1 = inters1.size
    val n2 = inters2.size
    println(s"Number of intersections: Is $n1 == $n2 ? ${n1 == n2}")

    val I1 = inters1.sortBy{ i => (i.p.x,  i.p.y) }
    val I2 = inters2.sortBy{ i => (i.getX, i.getY)}
    val sample = 10

    val stats = (I1 zip I2)
      .map{ case(i1, i2) =>
        (i1, i2, i1.point.distance(i2))
      }
    stats.take(10).foreach{ case(i1, i2, d) =>
      println{s"${i1.point.toText}\t${i2.toText}\t${d}"}
    }
    val S = stats.map(_._3)
    val max = S.max
    val sum = S.sum
    val len = S.length
    val avg = sum / len
    println(s"Avg: ${avg} Max: ${max}")

    logger.info(s"INFO|${runId}|${n1}|${n2}|${max}|${sum}|${len}|${avg}")
  }

  def sweepline2(hs1: List[Half_edge], hs2: List[Half_edge], debug: Boolean = false)
    (implicit geofactory: GeometryFactory): List[Intersection] = {

    val segs1 = hs1.map{ h => Segment(h, "A") }
    val segs2 = hs2.map{ h => Segment(h, "B") }
    val segs  = (segs1 ++ segs2)

    val intersector = BentleyOttmann2
    intersector.readSegments(segs)
    val intersections = intersector.getIntersections

    if(debug){
      save("/tmp/edgesS1.wkt"){
        segs1.map( s => s"${s.wkt}\n" )
      }
      save("/tmp/edgesS2.wkt"){
        segs2.map( s => s"${s.wkt}\n" )
      }
      save("/tmp/edgesI1.wkt"){
        intersections.zipWithIndex.map{ case (intersect, id)  =>
          val i = geofactory.createPoint(intersect.p)
          i.setUserData(id)
        
          val wkt = i.toText
          s"$wkt\t$id\n"
        }
      }
    }

    intersections
  }

  def sweeplineJTS2(hs1: List[Half_edge], hs2: List[Half_edge], debug: Boolean = false)
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
      if( iList.size == 0 ){
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
