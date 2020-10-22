package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geomgraph.index.SimpleMCSweepLineIntersector
import com.vividsolutions.jts.geomgraph.index.SegmentIntersector
import com.vividsolutions.jts.algorithm.RobustLineIntersector
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.geom.Coordinate
import com.vividsolutions.jts.geomgraph.EdgeIntersection
import com.vividsolutions.jts.geomgraph.Edge
import scala.collection.JavaConverters._
import edu.ucr.dblab.sdcel.geometries.{Half_edge, EdgeData}
import edu.ucr.dblab.sdcel.DCELMerger2.setTwins

object SweepLine3 {

  case class HEdge(coords: Array[Coordinate], h: Half_edge) extends Edge(coords)

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

  def main(args: Array[String]): Unit = {
    val model = new PrecisionModel(1000)
    implicit val geofactory = new GeometryFactory(model)

    val a1 = geofactory.createLineString(Array(new Coordinate(0,0), new Coordinate(2,0)))
    val a2 = geofactory.createLineString(Array(new Coordinate(2,0), new Coordinate(2,2)))
    val a3 = geofactory.createLineString(Array(new Coordinate(2,2), new Coordinate(0,2)))
    val a4 = geofactory.createLineString(Array(new Coordinate(0,2), new Coordinate(0,0)))
    a1.setUserData(EdgeData(1,0,0,false,"A"))
    a2.setUserData(EdgeData(1,0,1,false,"A"))
    a3.setUserData(EdgeData(1,0,2,false,"A"))
    a4.setUserData(EdgeData(1,0,3,false,"A"))
    val ha = List(a1,a2,a3,a4).map{Half_edge}
    ha.zip(ha.tail).foreach{ case(h1, h2) =>
      h1.next = h2
      h2.prev = h1
    }
    ha.head.prev = ha.last
    ha.last.next = ha.head

    val b1 = geofactory.createLineString(Array(new Coordinate(1,1), new Coordinate(3,1)))
    val b2 = geofactory.createLineString(Array(new Coordinate(3,1), new Coordinate(3,3)))
    val b3 = geofactory.createLineString(Array(new Coordinate(3,3), new Coordinate(1,3)))
    val b4 = geofactory.createLineString(Array(new Coordinate(1,3), new Coordinate(1,1)))
    b1.setUserData(EdgeData(2,0,0,false,"B"))
    b2.setUserData(EdgeData(2,0,1,false,"B"))
    b3.setUserData(EdgeData(2,0,2,false,"B"))
    b4.setUserData(EdgeData(2,0,3,false,"B"))
    val hb = List(b1,b2,b3,b4).map{Half_edge}
    hb.zip(hb.tail).foreach{ case(h1, h2) =>
      h1.next = h2
      h2.prev = h1
    }
    hb.head.prev = hb.last
    hb.last.next = hb.head
    /********************************************************/

    /********************************************************/
    val intersections = intersects(ha, hb)

    //intersections.foreach{println}
    val splits = intersections.flatMap{ case(p, hs) =>
      val h1 = hs.head
      val h2 = hs.last

      h1.split(p) ++ h2.split(p)
    }

    val hedges = setTwins(splits.toList)

    // HAVE TO BRING THE VERTICES INVOLVED TO HAVE ACCESS TO ALL PREVS AND NEXTS!!!
    val incidents = for{
      v <- intersections.keys
      h <- hedges ++ hedges.map(_.twin) if v == h.v2
    } yield {
      (v, h)
    }
    val hh = incidents.groupBy(_._1).mapValues{ pairs =>
      val hs = pairs.map(_._2).toList.sortBy(_.angleAtDest).reverse
      hs.zip(hs.tail).foreach{ case(h1, h2) =>
        h1.next = h2.twin
        h2.twin.prev = h1
      }
      hs.last.next = hs.head
      hs.head.prev = hs.last

      hs
    }.values.flatten

    val h = hh.groupBy(s => (s.data.label, s.data.polygonId)).values.map(_.head).toList
    h.foreach{println}
    List(h.toList(2)).foreach{ x =>
      println
      x.getNexts.map{ n =>
        val wkt = n.edge.toText
        val pid = n.data.polygonId
        val eid = n.data.edgeId
        s"$wkt\t$pid:$eid"
      }.foreach{println}
    }
    val intersectionsPoints = intersections.keySet.map{geofactory.createPoint}
    /********************************************************/

    /********************************************************/
    val vif = new java.io.PrintWriter("/tmp/edgesVi.wkt")
    vif.write(intersectionsPoints.map(_.toText).mkString("\n"))
    vif.close

    val sf = new java.io.PrintWriter("/tmp/edgesS.wkt")
    sf.write(hedges.map{ h =>
      val wkt1 = h.edge.toText
      val lab1 = h.label
      val wkt2 = h.twin.edge.toText
      val lab2 = h.twin.label
      s"$wkt1\t$lab1\n$wkt2\t$lab2\n"
    }.mkString(""))
    sf.close

    val hf = new java.io.PrintWriter("/tmp/edgesH.wkt")
    hf.write(
      h.map{ x =>
        val wkt = x.getNextsAsWKT
        val label = x.label

        s"$wkt\t$label\n"
      }.mkString("")
    )
    hf.close
    
    val haf = new java.io.PrintWriter("/tmp/edgesHa.wkt")
    haf.write(ha.head.getNextsAsWKT)
    haf.close

    val hbf = new java.io.PrintWriter("/tmp/edgesHb.wkt")
    hbf.write(hb(2).getNextsAsWKT)
    hbf.close
    
    val haif = new java.io.PrintWriter("/tmp/edgesHai.wkt")
    haif.write(
      intersections.valuesIterator.map{ pair =>
        val h = pair.head
        val wkt = h.edge.toText
        val pid = h.data.polygonId
        val eid = h.data.edgeId
        s"$wkt\t$pid:$eid\n"
      }.mkString("")
    )
    haif.close

    val hbif = new java.io.PrintWriter("/tmp/edgesHbi.wkt")
    hbif.write(
      intersections.valuesIterator.map{ pair =>
        val h = pair.last
        val wkt = h.edge.toText
        val pid = h.data.polygonId
        val eid = h.data.edgeId
        s"$wkt\t$pid:$eid\n"
      }.mkString("")
    )
    hbif.close    
    /********************************************************/
  }
}
