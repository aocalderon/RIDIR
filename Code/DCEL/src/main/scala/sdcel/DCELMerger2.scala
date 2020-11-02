package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geomgraph.index.SimpleMCSweepLineIntersector
import com.vividsolutions.jts.geomgraph.index.SegmentIntersector
import com.vividsolutions.jts.algorithm.RobustLineIntersector
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.geom.Coordinate
import com.vividsolutions.jts.geomgraph.EdgeIntersection
import com.vividsolutions.jts.geomgraph.Edge
import scala.collection.JavaConverters._
import edu.ucr.dblab.sdcel.geometries.{Half_edge, Vertex, EdgeData, HEdge, Tag}

object DCELMerger2 {

  def intersects(hedgesA: List[Half_edge], hedgesB: List[Half_edge],
    partitionId: Int = -1)
    (implicit geofactory: GeometryFactory): Map[Coordinate, List[Half_edge]] = {

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
    lineIntersector.setMakePrecise(geofactory.getPrecisionModel)
    val segmentIntersector = new SegmentIntersector(lineIntersector, true, true)

    sweepline.computeIntersections(aList, bList, segmentIntersector)

    (getIntersections(aList) ++ getIntersections(bList))
      .groupBy(_._1).mapValues(_.map(_._2).sortBy(_.data.label))
  }

  private def getIntersections(list: java.util.List[HEdge]):
      List[(Coordinate, Half_edge)] = {
    list.asScala.flatMap{ edge =>
      edge.getEdgeIntersectionList.iterator.asScala.map{ i =>
        (i.asInstanceOf[EdgeIntersection].getCoordinate, edge.h)
      }.toList
    }.toList
  }

  def setTwins(hedges: List[Half_edge])
    (implicit geofactory: GeometryFactory): List[Half_edge] = {

    case class H(vertex: Vertex, hedge: Half_edge, angle: Double)
    val Hs = hedges.flatMap{ h =>
      List(
        H(h.orig, h, h.angleAtOrig),
        H(h.dest, h, h.angleAtDest)
      )
    }
    val grouped = Hs.groupBy(h => (h.vertex, h.angle)).values.foreach{ hList =>
      val (h0, h1) = if(hList.size == 1) {
        val h0 = hList(0).hedge
        val h1 = h0.reverse
        (h0, h1)
      } else {
        val h0 = hList(0).hedge
        val h1 = hList(1).hedge
        (h0, h1)
      }

      h0.twin = h1
      h1.twin = h0
    }

    hedges
  }

  def merge(ha: List[Half_edge], hb: List[Half_edge])
    (implicit geofactory: GeometryFactory): Iterable[(String, Half_edge)] = {

    val intersections = intersects(ha, hb)

    val splits = intersections.map{ case(p, hList) =>
      hList.map(h => (h,p))
    }.flatten.groupBy(_._1).mapValues(_.map(_._2))
      .map{ case(k, v) =>
        val h = List(k)
        v.toList.foldLeft(h){ case(h, c) => h.map(_.split(c)).flatten}
      }.flatten.groupBy(h => (h.v1, h.v2)).mapValues{ h =>
      val tags = h.map(h => Tag(h.data.label, h.data.polygonId))
      val h_prime = h.head
      h_prime.tags = tags.toList
      h_prime
    }.values.toList

    val hedges = setTwins(splits)

    save("/tmp/edgesT.wkt", 
      (hedges ++ hedges.map(_.twin).filter(_.data.polygonId < 0)).map{ h =>
        val wkt = h.edge.toText
        val lab = h.label

        s"$wkt\t$lab\n"
      }
    )

    // Take half-edges and their twins...
    val incidents = (hedges ++ hedges.map(_.twin).filter(_.data.polygonId < 0))
      .groupBy(_.v2) // Group them by the destination vertex...
      .filter(_._2.size > 1) // Remove isolate vertices
                             // (those with less than 2 incident half-edges)

    //println("Incidents")
    //incidents.foreach{println}

    //println

    // At each vertex, get their incident half-edges...
    val hh = incidents.mapValues{ hList =>
      // Sort them by angle...
      val hs = hList.sortBy(- _.angleAtDest)
      // Add first incident to comple the sequence...
      val hs_prime = hs :+ hs.head
      // zip and tail will pair each half-edge with its next one...
      hs_prime.zip(hs_prime.tail).foreach{ case(h1, h2) =>
        h1.next = h2.twin
        h2.twin.prev = h1
      }

      hs
    }.values.flatten.filter(_.data.polygonId >= 0)

    //hh.map(h => (h, h.tags, h.updateTags)).foreach{println}

    val h = hh.map{ h =>
      h.tags = h.updateTags
      (h.getTag, h)
    }.groupBy(_._1).values.map(_.head)

    h
  }

  def save(name: String, content: Seq[String]): Unit = {
    val hf = new java.io.PrintWriter(name)
    hf.write(content.mkString(""))
    hf.close
    println(s"Saved $name [${content.size} records].")
  }
}
