package edu.ucr.dblab.debug

import scala.collection.mutable.{PriorityQueue}
import scala.collection.JavaConverters._

import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory, Coordinate, Point}
import com.vividsolutions.jts.geomgraph.index.SimpleMCSweepLineIntersector
import com.vividsolutions.jts.geomgraph.index.SegmentIntersector
import com.vividsolutions.jts.algorithm.RobustLineIntersector
import com.vividsolutions.jts.geomgraph.EdgeIntersection
import com.vividsolutions.jts.geomgraph.Edge

import edu.ucr.dblab.bo._
import edu.ucr.dblab.debug.BO.{generateRandomHedges, generateFromFile, sweepline}
import edu.ucr.dblab.sdcel.geometries.{Half_edge, HEdge, Seg}
import edu.ucr.dblab.sdcel.geometries.{EventPoint, EventPoint_Ordering, CoordYX_Ordering}
import edu.ucr.dblab.sdcel.geometries.{LEFT_ENDPOINT, INTERSECT, RIGHT_ENDPOINT}
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

    println(s"METHOD:     $method   ")
    println(s"TOLERANCE:  $tolerance")
    println(s"SCALE:      ${geofactory.getPrecisionModel.getScale}")
    println(s"N:          $n        ")
    println(s"FILE:       $filename ")
    println(s"DEBUG:      $debug    ")

    val hs1 = generateRandomHedges(n)
    val hs2 = generateRandomHedges(n, "B")
    //val hs1 = generateFromFile("/home/acald013/tmp/g1.wkt")
    //val hs2 = generateFromFile("/home/acald013/tmp/g2.wkt")

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

    val I1 = inters1.sortBy{ i => (i.getCoordinate.x,  i.getCoordinate.y) }
    val I2 = inters2.sortBy{ i => (i.getX, i.getY)}
    val sample = 10

    val stats = (I1 zip I2)
      .map{ case(i1, i2) =>
        (i1, i2, i1.getCoordinate.distance(i2.getCoordinate))
      }
    stats.take(10).foreach{ case(i1, i2, d) =>
      println{s"${i1.getPoint}\t${i2.toText}\t${d}"}
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

    val edges1 = hs1.map{ h =>
      val seg = Seg(h).edge
      seg.setLabel("A")
      seg.setId(h.id)
      seg
    }
    val edges2 = hs2.map{ h =>
      val seg = Seg(h).edge
      seg.setLabel("B")
      seg.setId(h.id)
      seg
    }
    val edges = edges1 ++ edges2

    val intersector = new BentleyOttmann(edges.asJava)
    val intersections = intersector.getIntersections

    if(debug){
      save("/tmp/edgesS1.wkt"){
        edges1.map(e => s"${e}\n")
      }
      save("/tmp/edgesS2.wkt"){
        edges2.map(e => s"${e}\n")
      }
      save("/tmp/edgesI1.wkt"){
        intersections.asScala.zipWithIndex.map{ case (intersect, id)  =>
          val i = geofactory.createPoint(intersect.getCoordinate)
          i.setUserData(id)
        
          val wkt = i.toText
          s"$wkt\t$id\n"
        }
      }
    }

    intersections.asScala.toList
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
