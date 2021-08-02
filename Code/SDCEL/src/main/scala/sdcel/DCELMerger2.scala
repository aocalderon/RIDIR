package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geomgraph.index.SimpleMCSweepLineIntersector
import com.vividsolutions.jts.geomgraph.index.SegmentIntersector
import com.vividsolutions.jts.algorithm.RobustLineIntersector
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.geom.Coordinate
import com.vividsolutions.jts.geomgraph.EdgeIntersection
import com.vividsolutions.jts.geomgraph.Edge
import scala.collection.JavaConverters._
import scala.annotation.tailrec
import edu.ucr.dblab.sdcel.geometries.{Half_edge, Vertex, EdgeData, HEdge, Tag}

import Utils._

object DCELMerger2 {

  def intersects(hedgesA: List[Half_edge], hedgesB: List[Half_edge])
    (implicit geofactory: GeometryFactory): Map[Coordinate, List[Half_edge]] = {
    val pid = org.apache.spark.TaskContext.getPartitionId

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

    (getIntersections(aList) ++ getIntersections(bList))
      .groupBy(_._1).mapValues(_.map(_._2).sortBy(_.data.label))
  }

  private def getIntersections(list: java.util.List[HEdge])
    (implicit geofactory: GeometryFactory): List[(Coordinate, Half_edge)] = {
    list.asScala.flatMap{ edge =>
      edge.getEdgeIntersectionList.iterator.asScala.map{ i =>
        val coord = i.asInstanceOf[EdgeIntersection].getCoordinate
        (coord, edge.h)
      }.toList
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
    hedges
  }

  def merge2(ha: List[Half_edge], hb: List[Half_edge], debug: Boolean = false)
    (implicit geofactory: GeometryFactory): Iterable[(Half_edge, String)] = {
    val pid = org.apache.spark.TaskContext.getPartitionId

    val partitionId = 29

    // Getting intersection between dcel A and B...
    val intersections = intersects(ha, hb)

    if(pid == partitionId){
      println(s"Intersections")
      intersections.foreach(println)
    }

    if(debug){
      val inters_prime = intersections.zipWithIndex
      save(s"/tmp/edgesC$pid.wkt",
        inters_prime.map{ case(inters, id) =>
          val (c, hList) = inters
          val wkt = geofactory.createPoint(c).toText
          val n = hList.size
          val hwkt = hList.map(_.wkt).mkString(" ")

          s"$wkt\t$id\t$n\t$hwkt\n"
        }.toList
      )
      save(s"/tmp/edgesE$pid.wkt",
        inters_prime.map{ case(inters, id) =>
          val (c, hList) = inters
          val wkt1 = geofactory.createPoint(c).toText
          hList.map{ h =>
            val wkt = h.wkt
            val data = h.data

            s"$wkt\t$data\t$id\t$wkt1\n"
          }
        }.flatten.toList
      )
    }
     
    // Split the half-edges which intersect each other...
    val splits0 = intersections.map{ case(p, hList) =>
      hList.map(h => (h,p))
    }.flatten.groupBy(_._1).mapValues(_.map(_._2))

    if(pid == 1){
        println(s"Splits at 3")
        splits0.foreach(println)
      }

    val splits1 = splits0.map{ case(k, v) =>
      val h = List(k)
      // traverse the half-edge splitting at each coordinate
      v.toList.foldLeft(h){ case(h, c) => h.map(_.split(c)).flatten}
    }.flatten.groupBy(h => (h.v1, h.v2))

    if(pid == 1){
        println(s"Splits at 3")
        splits1.foreach(println)
      }

    val splits = splits1.mapValues{ h =>
      val tags = h.map(h => Tag(h.data.label, h.data.polygonId))
      val h_prime = h.head
      h_prime.tags = tags.toList
      h_prime
    }.values.toList

    if(debug)
      save(s"/tmp/edgesS$pid.wkt",
        splits.map{ h =>
          val wkt = h.wkt
          val data = h.data
          
          s"$wkt\t$data\n"
        }
      )

    // Remove the half-edges which intersect and replace them with their splits...
    val hedges_prime = ((ha ++ hb).toSet -- intersections.values.flatten).toList ++ splits

    // Match half-edges with their twins (create them if needed)...
    val hedges = setTwins(hedges_prime).filter(_.twin != null)
    // Extract set of vertices...
    val vertices = hedges.map(_.orig).distinct
    
    if(debug){
      save(s"/tmp/edgesV$pid.wkt",
        vertices.map{ v =>
          val wkt = v.toPoint.toText
          
          s"$wkt\n"
        }.toList
      )
      save("/tmp/edgesHedges.wkt",
        hedges.map{ h =>
          val wkt = h.wkt
          val pid = h.data.polygonId
          val eid = h.data.edgeId
          s"$wkt\t$pid\t$eid\n"
        }
      )
      save("/tmp/edgesTwins.wkt",
        hedges.map{ h =>
          val (wkt, pid, eid, wkt1, pid1, eid1) = if(h.twin != null)
            (h.wkt, h.data.polygonId, h.data.edgeId,
              h.twin.wkt, h.twin.data.polygonId, h.twin.data.edgeId)
          else
            (h.wkt, h.data.polygonId, h.data.edgeId,
              "LINESTRING EMPTY", -9, -9)
          s"$wkt\t$pid\t$eid\t$wkt1\t$pid1\t$eid1\n"
        }
      )
    }

    // Group half-edges by the destination vertex (v2)...
    val incidents = try {
      (hedges ++ hedges.map(_.twin)).groupBy(_.v2).values.toList
    } catch {
      case e: java.lang.NullPointerException => {
        // Why are you null?
        println(e)
        println("Null twin:")
        hedges.filter(h => h.twin == null).foreach{println}
        System.exit(0)
        hedges.groupBy(_.v2).values.toList
      }
    }

    if(debug)
      save(s"/tmp/edgesI$pid.wkt",
        incidents.map{ case(hedges) =>
          val labs = hedges.map(_.label).sorted
          val wkt = geofactory.createMultiLineString(hedges.map(_.edge).toArray).toText

          s"$wkt\t$labs\n"
        }.toList
      )

    // At each vertex, get their incident half-edges...
    val h_prime = incidents.map{ hList =>
      // Sort them by angle...
      val hs = hList.sortBy(- _.angleAtDest)
      // Add first incident to complete the sequence...
      val hs_prime = hs :+ hs.head
      // zip and tail will pair each half-edge with its next one...
      hs_prime.zip(hs_prime.tail).foreach{ case(h1, h2) =>
        h1.next = h2.twin
        h2.twin.prev = h1
      }

      hs
    }.flatten.filter{ h =>
      intersections.keySet.contains(h.v2)
    }.filter(_.data.polygonId != -1).toSet

    if(pid == 1){
      println("h_prime at 3:")
      h_prime.map(h => (h, h.getTag)).foreach(println)
    }

    if(debug){
      println("h_prime: " + h_prime.size)
      save("/tmp/edgesHprime.wkt",
        h_prime.map{ h =>
          h.wkt + "\n"
        }.toList
      )
    }

    // Group by next...
    val h = groupByNext(h_prime, List.empty[(Half_edge, String)])
      .filter(_._2 != "")

    if(pid == 1){
      println("h")
      h.foreach{println}
    }

    if(false){
      println(s"PID: $pid")
      val labels = h_prime.map{_.getNexts.map{_.label}.mkString(" ")}.mkString("\n")
      println(s"$labels \n")
      println("H size: " + h.size)
      save(s"/tmp/edgesH_prime$pid.wkt",
        h.map{ case(h, tag) =>
          val wkt = h.getNextsAsWKT

          s"$wkt\t$tag\n"
        }.toList
      )
    }

    h
  }

  @tailrec
  def groupByNext(hs: Set[Half_edge], r: List[(Half_edge, String)])
      : List[(Half_edge, String)] = {

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
        val hs_new = hs -- h.getNexts.toSet
        //h.tags = h.updateTags
        val r_new  = r :+ ((h, labels))

        groupByNext(hs_new, r_new)
      }
    }
  }

  def save(name: String, content: Seq[String]): Unit = {
    val hf = new java.io.PrintWriter(name)
    hf.write(content.mkString(""))
    hf.close
    println(s"Saved $name [${content.size} records].")
  }

  def merge3(ha: List[Half_edge], hb: List[Half_edge], debug: Boolean = false)
    (implicit geofactory: GeometryFactory): Iterable[(Half_edge, String)] = {
    val pid = org.apache.spark.TaskContext.getPartitionId

    val partitionId = 29

    // Getting intersection between dcel A and B...
    val intersections = intersects(ha, hb)

    if(debug){
      val inters_prime = intersections.zipWithIndex
      save(s"/tmp/edgesC$pid.wkt",
        inters_prime.map{ case(inters, id) =>
          val (c, hList) = inters
          val wkt = geofactory.createPoint(c).toText
          val n = hList.size
          val hwkt = hList.map(_.wkt).mkString(" ")

          s"$wkt\t$id\t$n\t$hwkt\n"
        }.toList
      )
      save(s"/tmp/edgesE$pid.wkt",
        inters_prime.map{ case(inters, id) =>
          val (c, hList) = inters
          val wkt1 = geofactory.createPoint(c).toText
          hList.map{ h =>
            val wkt = h.wkt
            val data = h.data

            s"$wkt\t$data\t$id\t$wkt1\n"
          }
        }.flatten.toList
      )
    }
     
    // Split the half-edges which intersect each other...
    val splits0 = intersections.map{ case(p, hList) =>
      hList.map(h => (h,p))
    }.flatten.groupBy(_._1).mapValues(_.map(_._2))

    if(pid == partitionId){
        //println(s"Splits at 3")
        //splits0.foreach(println)
      }

    val splits1 = splits0.map{ case(k, v) =>
      val h = List(k)
      // traverse the half-edge splitting at each coordinate
      v.toList.foldLeft(h){ case(h, c) => h.map(_.split(c)).flatten}
    }.flatten.groupBy(h => (h.v1, h.v2))

    if(pid == partitionId){
        //println(s"Splits")
        //splits1.foreach(println)
      }

    val splits = splits1.mapValues{ h =>
      val tags = h.map(h => Tag(h.data.label, h.data.polygonId))
      val h_prime = h.head
      h_prime.tags = tags.toList
      h_prime
    }.values.toList

    if(debug)
      save(s"/tmp/edgesS$pid.wkt",
        splits.map{ h =>
          val wkt = h.wkt
          val data = h.data
          
          s"$wkt\t$data\n"
        }
      )

    // Remove the half-edges which intersect and replace them with their splits...
    val hedges_prime = ((ha ++ hb).toSet -- intersections.values.flatten).toList ++ splits

    // Match half-edges with their twins (create them if needed)...
    val hedges = setTwins(hedges_prime).filter(_.twin != null)
    // Extract set of vertices...
    val vertices = hedges.map(_.orig).distinct
    
    if(debug){
      save(s"/tmp/edgesV$pid.wkt",
        vertices.map{ v =>
          val wkt = v.toPoint.toText
          
          s"$wkt\n"
        }.toList
      )
      save("/tmp/edgesHedges.wkt",
        hedges.map{ h =>
          val wkt = h.wkt
          val pid = h.data.polygonId
          val eid = h.data.edgeId
          s"$wkt\t$pid\t$eid\n"
        }
      )
      save("/tmp/edgesTwins.wkt",
        hedges.map{ h =>
          val (wkt, pid, eid, wkt1, pid1, eid1) = if(h.twin != null)
            (h.wkt, h.data.polygonId, h.data.edgeId,
              h.twin.wkt, h.twin.data.polygonId, h.twin.data.edgeId)
          else
            (h.wkt, h.data.polygonId, h.data.edgeId,
              "LINESTRING EMPTY", -9, -9)
          s"$wkt\t$pid\t$eid\t$wkt1\t$pid1\t$eid1\n"
        }
      )
    }

    // Group half-edges by the destination vertex (v2)...
    val incidents = try {
      (hedges ++ hedges.map(_.twin)).groupBy(_.v2).values.toList
    } catch {
      case e: java.lang.NullPointerException => {
        // Why are you null?
        println(e)
        println("Null twin:")
        hedges.filter(h => h.twin == null).foreach{println}
        System.exit(0)
        hedges.groupBy(_.v2).values.toList
      }
    }

    if(debug)
      save(s"/tmp/edgesI$pid.wkt",
        incidents.map{ case(hedges) =>
          val labs = hedges.map(_.label).sorted
          val wkt = geofactory.createMultiLineString(hedges.map(_.edge).toArray).toText

          s"$wkt\t$labs\n"
        }.toList
      )

    // At each vertex, get their incident half-edges...
    val h_prime = incidents.map{ hList =>
      // Sort them by angle...
      val hs = hList.sortBy(- _.angleAtDest)
      // Add first incident to complete the sequence...
      val hs_prime = hs :+ hs.head
      // zip and tail will pair each half-edge with its next one...
      hs_prime.zip(hs_prime.tail).foreach{ case(h1, h2) =>
        h1.next = h2.twin
        h2.twin.prev = h1
      }

      hs
    }.flatten.filter{ h =>
      intersections.keySet.contains(h.v2)
    }//.filter(_.data.polygonId != -1)
      .toSet

    if(pid == partitionId){
      //println("h_prime at 3:")
      //h_prime.map(h => (h, h.getTag)).foreach(println)
    }

    if(debug){
      println("h_prime: " + h_prime.size)
      save("/tmp/edgesHprime.wkt",
        h_prime.map{ h =>
          h.wkt + "\n"
        }.toList
      )
    }

    // Group by next...
    val h = groupByNext(h_prime, List.empty[(Half_edge, String)])
      .filter(_._2 != "")

    if(pid == partitionId){
      println("Done.")
      //h.foreach{println}
    }

    h
  }


  def intersects4(hedgesA: List[Half_edge], hedgesB: List[Half_edge], partitionId: Int = -1)
    (implicit geofactory: GeometryFactory)
      : (List[Half_edge], List[Half_edge]) = {
    val pid = org.apache.spark.TaskContext.getPartitionId

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
    /*
    try{
      sweepline.computeIntersections(aList, bList, segmentIntersector)
    } catch{
      case e: java.lang.IllegalArgumentException => {
        println(s"PID: ${pid}")
      }
    }
     */

    val aHedges = aList.asScala.flatMap{ a =>
      val iList = a.getEdgeIntersectionList.iterator.asScala.toList
      if(iList.size == 0){
        List(a.h)
      } else {
        val coords_prime = iList.map{ i =>
          val coord = i.asInstanceOf[EdgeIntersection].getCoordinate
          coord
        }.toList
        val original_hedge = a.h
        val coords = (a.h.v1 +: coords_prime :+ a.h.v2).distinct
        val hedges = coords.zip(coords.tail).map{ case(a1, a2) =>
          val l = geofactory.createLineString(Array(a1, a2))
          l.setUserData(original_hedge.data)
          val h = Half_edge(l)
          h
        }
        hedges
      }
    }.toList

    val bHedges = bList.asScala.flatMap{ a =>
      val iList = a.getEdgeIntersectionList.iterator.asScala.toList
      if(iList.size == 0){
        List(a.h)
      } else {
        val coords_prime = iList.map{ i =>
          val coord = i.asInstanceOf[EdgeIntersection].getCoordinate
          coord
        }.toList
        val original_hedge = a.h
        val coords = (a.h.v1 +: coords_prime :+ a.h.v2).distinct
        val hedges = coords.zip(coords.tail).map{ case(a1, a2) =>
          val l = geofactory.createLineString(Array(a1, a2))
          l.setUserData(original_hedge.data)
          val h = Half_edge(l)
          h
        }
        hedges
      }
    }.toList

    (aHedges, bHedges)

  }

  def merge4(ha: List[Half_edge], hb: List[Half_edge], debug: Boolean = false)
      (implicit geofactory: GeometryFactory)
  : List[(Half_edge,String)] = {

    val pid = org.apache.spark.TaskContext.getPartitionId
    val partitionId = 16

    // Getting edge splits...
    val (aList, bList) = intersects4(ha, hb, partitionId)
    val hedges_prime = (aList ++ bList)

    // Remove duplicates...
    val hedges = hedges_prime.groupBy{h => (h.v1, h.v2)}.values.map(_.head).toList

    // Setting new twins...
    val hedges2 = setTwins(hedges).filter(_.twin != null)

    // Running sequential...
    sequential(hedges2, partitionId)

    // Extrantinct unique half-edge and label to represent the face...
    val h = groupByNext(hedges.toSet, List.empty[(Half_edge, String)])
      .filter(_._2 != "")

    h

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
    val incidents = hedges.groupBy(_.v2).values.toList

    if(pid == partitionId){
      save("/tmp/edgesI.wkt",
        incidents.map{ h =>
          val i = geofactory.createPoint(h.head.v2)
          s"${i.toText}\t${h.map(_.wkt).mkString(" ")}\n"
        }
      )
    }

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
}
