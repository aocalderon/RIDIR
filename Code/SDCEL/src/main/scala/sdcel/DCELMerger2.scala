package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geomgraph.index.SimpleMCSweepLineIntersector
import com.vividsolutions.jts.geomgraph.index.SegmentIntersector
import com.vividsolutions.jts.algorithm.RobustLineIntersector
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.geom.{Polygon, Coordinate, Envelope}
import com.vividsolutions.jts.geomgraph.EdgeIntersection
import com.vividsolutions.jts.geomgraph.Edge
import com.vividsolutions.jts.index.strtree._

import scala.collection.JavaConverters._
import scala.annotation.tailrec

import org.apache.spark.TaskContext

import edu.ucr.dblab.sdcel.geometries.{Half_edge, Vertex, EdgeData, HEdge, Tag, Cell}
import Utils._
import PartitionReader.{envelope2polygon}
import org.slf4j.{Logger, LoggerFactory}

object DCELMerger2 {
  def merge(hlepA: List[(Half_edge,String,Envelope, Polygon)],
    hlepB: List[(Half_edge,String,Envelope, Polygon)], cells: Map[Int, Cell])
      (implicit geofactory: GeometryFactory, settings: Settings, logger: Logger)
      : List[(Half_edge,String)] = {

    // Getting edge splits...
    val ha = extractHedges(hlepA)
    val hb = extractHedges(hlepB)
    val (aList, bList) = intersects(ha, hb)
    val hedges0 = (aList ++ bList)

    // Remove duplicates...
    val hedges1 = removeDuplicates(hedges0)

    // Setting new twins...
    val hedges2 = setTwins(hedges1)

    // Running sequential...
    sequential(hedges2)

    // Getting unique half-edge and label to represent the face...
    val hedges3 = groupByNextMBR(hedges1.toSet, List.empty[(Half_edge, String, Envelope)])

    val Artree = getRTree(hlepA)
    val Brtree = getRTree(hlepB)
    val (singles, multiples) = hedges3.partition{ case(h,l,e) =>
      l.split(" ").size == 1
    }
    val (singlesA, singlesB) = singles.partition{ case(h,l,e) =>
      l.substring(0,1) == "A"
    }
    val newSinglesA = updateLabel(singlesA, Brtree)
    val newSinglesB = updateLabel(singlesB, Artree)

    val hedges4 = newSinglesA ++ newSinglesB ++ multiples.map{ case(h,l,e) => (h,l)}
    
    hedges4
  }

  def updateLabel(singles: List[(Half_edge, String, Envelope)], rtree: STRtree)
    (implicit geofactory: GeometryFactory): List[(Half_edge, String)] = {
    val newSingles = singles.map{ case(h,l,e) =>
      val answers = rtree.query(e).asScala.toList.filter{ b =>
        b.asInstanceOf[Half_edge].mbr.contains(e)
      }

      if(answers.isEmpty){
        (h,l)
      } else {
        val a = answers.map{ b =>
          val h1 = b.asInstanceOf[Half_edge]
          val l1 = h1.label
          val bool = try {
            h.getPolygon.coveredBy(h1.poly)
          } catch {
            case e: com.vividsolutions.jts.geom.TopologyException => false
          }

          (bool, l1)
        }
        val tt = a.filter(_._1)
        val nl = if( tt.isEmpty ){
          l
        } else {
          val l2 = tt.head._2
          List(l, l2).sorted.mkString(" ")
        }

        (h,nl)
      }
    }
    newSingles
  }

  @tailrec
  def groupByNextMBRPoly(hs: Set[Half_edge],
    r: List[(Half_edge, String, Envelope, Polygon)])
    (implicit geofactory: GeometryFactory, settings: Settings, logger: Logger)
      : List[(Half_edge, String, Envelope, Polygon)] = {

    if(hs.isEmpty) {
      r
    } else {
      val h = hs.head

      val (nexts, mbr, poly) = h.getNextsMBRPoly
      val labels = nexts.map{_.label}.distinct
        .filter(_ != "A")
        .filter(_ != "B").sorted.mkString(" ")
      if(nexts.isEmpty){
        r :+ ((h, "Error", new Envelope(), null))
      } else {
        val hs_new = hs -- nexts.toSet
        val r_new  = r :+ ((h, labels, mbr, poly))

        groupByNextMBRPoly(hs_new, r_new)
      }
    }
  }

  @tailrec
  def groupByNextMBR(hs: Set[Half_edge],
    r: List[(Half_edge, String, Envelope)]): List[(Half_edge, String, Envelope)] = {

    if(hs.isEmpty) {
      r.filter(_._2 != "")
    } else {
      val h = hs.head

      val (nexts,mbr, label) = h.getNextsMBR
      if(nexts.isEmpty){
        r :+ ((h, "Error", new Envelope()))
      } else {
        val hs_new = hs -- nexts.toSet
        val r_new  = r :+ ((h, label, mbr))

        groupByNextMBR(hs_new, r_new)
      }
    }
  }

  @tailrec
  def groupByNext(hs: Set[Half_edge],
    r: List[(Half_edge, String)]): List[(Half_edge, String)] = {

    if(hs.isEmpty) {
      r
    } else {
      val h = hs.head

      val nexts = h.getNexts
      val labels = nexts.map{_.label}.distinct
        .filter(_ != "A")
        .filter(_ != "B").sorted.mkString(" ")
      if(nexts.isEmpty){
        r :+ ((h, "Error"))
      } else {
        val hs_new = hs -- nexts.toSet
        //h.tags = h.updateTags
        val r_new  = r :+ ((h, labels))

        groupByNext(hs_new, r_new)
      }
    }
  }

  // Sequential implementation of dcel...
  def sequential(hedges_prime: List[Half_edge], partitionId: Int = -1)
      (implicit geofactory: GeometryFactory): Unit = {
    val pid = org.apache.spark.TaskContext.getPartitionId
    // Group half-edges by the destination vertex (v2)...

    val hedges = try{
      hedges_prime ++ hedges_prime.filter(_.twin.isNewTwin).map(_.twin)
    } catch {
      case e: java.lang.NullPointerException => {
        hedges_prime.filter(_.twin != null) ++ hedges_prime.filter(_.twin != null)
          .filter(_.twin.isNewTwin).map(_.twin)
      }
    }
    val incidents = hedges.groupBy(_.target).values.toList

    // At each vertex, get their incident half-edges...
    val h_prime = incidents.map{ hList =>
      // Sort them by angle...
      val hs = hList.distinct.sortBy(- _.angleAtDest)
      // Add first incident to complete the sequence...
      val hs_prime = hs :+ hs.head
      // zip and tail will pair each half-edge with its next one...
      hs_prime.zip(hs_prime.tail).foreach{ case(h1, h2) =>
        h1.next = h2.twin
        h2.twin.prev = h1
      }

      hs
    }.flatten
  }

  def intersects(hedgesA: List[Half_edge], hedgesB: List[Half_edge], partitionId: Int = -1)
    (implicit geofactory: GeometryFactory, settings: Settings)
      : (List[Half_edge], List[Half_edge]) = {

    val aList = hedgesA.map{ h =>
      val pts = Array(h.v1, h.v2)
      HEdge(pts, h)
    }.asJava

    val bList = hedgesB.map{ h =>
      val pts = Array(h.v1, h.v2)
      HEdge(pts, h)
    }.asJava
    
    val sweepline = new SimpleMCSweepLineIntersector()
    val lineIntersector = new RobustLineIntersector()
    val segmentIntersector = new SegmentIntersector(lineIntersector, true, true)
    sweepline.computeIntersections(aList, bList, segmentIntersector)

    val aHedges = getHalf_edges(aList)
    val bHedges = getHalf_edges(bList)

    (aHedges, bHedges)

  }

  def getHalf_edges(aList: java.util.List[HEdge])
    (implicit geofactory: GeometryFactory): List[Half_edge] = {

    aList.asScala.flatMap{ a =>
      val iList = a.getEdgeIntersectionList.iterator.asScala.toList
      if(iList.size == 0){
        List(a.h)
      } else {
        val coords_prime = iList.map{ i =>
          val coord = i.asInstanceOf[EdgeIntersection].getCoordinate
          Vertex(coord)
        }.toList
        val original_hedge = a.h
        val cp = coords_prime
          .filterNot(_.coord == a.h.orig.coord)
          .filterNot(_.coord == a.h.dest.coord)
        val coords = (a.h.orig +: cp :+ a.h.dest)
        val hedges = coords.zip(coords.tail).map{ case(a1, a2) =>
          val l = geofactory.createLineString(Array(a1.coord, a2.coord))
          l.setUserData(original_hedge.data)
          val h = Half_edge(l)
          h.tag = original_hedge.tag
          h.orig = a1
          h.dest = a2
          h.source = a1.coord
          h.target = a2.coord
          h
        }
        hedges
      }
    }.toList
  }

  /* Pair a list of half-edges with their twins */
  def setTwins(hedges: List[Half_edge])
    (implicit geofactory: GeometryFactory): List[Half_edge] = {

    case class H(hedge: Half_edge, start: Vertex, end: Vertex)

    // Get a copy of the half-edge by their vertices and angles...
    val Hs = hedges.flatMap{ h =>
      List(
        H(h, h.orig, h.dest),
        H(h, h.dest, h.orig)
      )
    }

    // Group by its vertex and angle...
    val grouped = Hs.groupBy(h => (h.start, h.end)).values.map{ hList =>
      val (h0, h1) = if(hList.size == 1) {
        // If at a vertex, a half-edge is alone at its angle, we create its twin...
        val h0 = hList(0).hedge
        val h1 = h0.reverse
        (h0, h1)
      } else {
        // At each vertex, if there are two half-edges at the same angle, they are twins...
        val h0 = hList(0).hedge
        val h1 = hList(1).hedge
        (h0, h1)
      }

      // Setting the twins...
      h0.twin = h1
      h1.twin = h0

      (h0, h1)
    }

    // Let's return the modified half-edges...
    hedges.filter(_.twin != null)
  }

  def extractHedges(hlep: List[(Half_edge,String,Envelope, Polygon)]): List[Half_edge] = {
    hlep.map{ case(hs,lab,env,pol) =>
      hs.getNexts.map{ h =>
        h.tag = lab
        h
      }
    }.flatten
  }

  def removeDuplicates(hs: List[Half_edge]): List[Half_edge] = {
    hs.groupBy{h => (h.source, h.target)}.values.map{ hs =>
      if(hs.size == 1){
        val h = hs.head
        h
      } else {
        val h = hs.head
        val l = hs.last
        val t1 = h.tag
        val t2 = l.tag
        h.tag = List(t1, t2).sorted.mkString(" ")
        h
      }
    }.toList
  }

  def getRTree(hlep: List[(Half_edge,String,Envelope, Polygon)]): STRtree = {
    val rtree = new STRtree()
    hlep.foreach{ case(h,l,e,p) =>
      h.mbr = e
      h.poly = p
      rtree.insert(e, h)
    }
    rtree
  }
}
