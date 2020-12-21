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

object DCELMerger2 {

  def intersects(hedgesA: List[Half_edge], hedgesB: List[Half_edge])
    (implicit geofactory: GeometryFactory): Map[Coordinate, List[Half_edge]] = {
    val pid = org.apache.spark.TaskContext.getPartitionId

    save("/tmp/edgesHedgesA.wkt", hedgesA.map(a => a.wkt + "\t" + a.data + "\n"))
    save("/tmp/edgesHedgesB.wkt", hedgesB.map(b => b.wkt + "\t" + b.data + "\n"))
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

  /* Pair a list of half-edges with their twins */
  def setTwins(hedges: List[Half_edge])
    (implicit geofactory: GeometryFactory): List[Half_edge] = {

    case class H(hedge: Half_edge, vertex: Vertex, angle: Double)
    // Get a copy of the half-edge by their vertices and angles...
    val Hs = hedges.flatMap{ h =>
      List(
        H(h, h.orig, h.angleAtOrig),
        H(h, h.dest, h.angleAtDest)
      )
    }
    // Group by its vertex and angle...
    val grouped = Hs.groupBy(h => (h.vertex, h.angle)).values.foreach{ hList =>
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
    }

    // Let's return the modified half-edges...
    hedges
  }

  def merge2(ha: List[Half_edge], hb: List[Half_edge], debug: Boolean = false)
    (implicit geofactory: GeometryFactory): Iterable[(Half_edge, String)] = {
    val pid = org.apache.spark.TaskContext.getPartitionId

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
          s"$wkt\n"
        }
      )
      save("/tmp/edgesTwins.wkt",
        hedges.map{ h =>
          val wkt = if(h.twin != null) h.twin.wkt else "LINESTRING EMPTY"
          s"$wkt\n"
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

    println("h_prime: " + h_prime.size)
    save("/tmp/edgesHprime.wkt",
      h_prime.map{ h =>
        h.wkt + "\n"
      }.toList
    )

    //
    val h = groupByNext(h_prime, List.empty[(Half_edge, String)])
      .filter(_._2 != "")

    println("H size: " + h.size)


    if(debug)
      save(s"/tmp/edgesH_prime$pid.wkt",
        h.map{ case(h, tag) =>
          val wkt = h.getNextsAsWKT

          s"$wkt\t$tag\n"
        }.toList
      )

    h
  }

  @tailrec
  def groupByNext(hs: Set[Half_edge], r: List[(Half_edge, String)]):
      List[(Half_edge, String)] = {

    println(hs.size)

    if(hs.isEmpty) {
      r
    } else {
      val h = hs.head

      val tag = if(h.data.label == "A")
        h.label + " " + h.next.label
      else
        h.next.label + " " + h.label
      val tag2 = List(h.label, h.next.label)
        .filterNot(t => t == "A" || t == "B").sorted.mkString(" ")

      val hs_new = hs -- h.getNexts.toSet
      val r_new  = r :+ ((h, tag2))

      groupByNext(hs_new, r_new)
    }
  }

  def merge(ha: List[Half_edge], hb: List[Half_edge])
    (implicit geofactory: GeometryFactory): Iterable[(String, Half_edge)] = {

    val intersections = intersects(ha, hb)

    /* Debug */
    save("/tmp/edgesI.wkt", 
      intersections.keys.map{ c =>
        val wkt = geofactory.createPoint(c).toText

        s"$wkt\n"
      }.toList
    )
    /* Debug */

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

    /* Debug */
    save("/tmp/edgesT.wkt", 
      (hedges ++ hedges.map(_.twin).filter(_.data.polygonId < 0)).map{ h =>
        val wkt = h.edge.toText
        val lab = h.label

        s"$wkt\t$lab\t${h.prev}\n"
      }
    )
    /* Debug */

    // Take half-edges and their twins...
    val incidents = (hedges ++ hedges.map(_.twin).filter(_.data.polygonId < 0))
      .groupBy(_.v2) // Group them by the destination vertex...
      .filter(_._2.size > 1) // Remove isolate vertices
                             // (those with less than 2 incident half-edges)
    .values.toList

    /* Debug */
    save("/tmp/edgesIn.wkt", 
      incidents.map{ case(hedges) =>
        val labs = hedges.map(_.label).sorted
        val wkt = geofactory.createMultiLineString(hedges.map(_.edge).toArray).toText

        val As = labs.filter(_.contains("A")).size
        val Bs = labs.filter(_.contains("B")).size
        val ls = hedges.map(_.data.label)
        val AB = ls.exists(_ == "A") && ls.exists(_ == "B")
        val ABs = labs.filter(l => l.contains("A") && l.contains("B")).size

        s"$wkt\t${labs.size}\t$As\t$Bs\t$AB\t$ABs\t${labs.mkString(", ")}\n"
      }.toList
    )
    /* Debug */

    // At each vertex, get their incident half-edges...
    val hh = incidents.map{ hList =>
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
    }.flatten.filter(_.data.polygonId >= 0)

    //hh.map(h => (h, h.tags, h.updateTags)).foreach{println}
    /* Debug */
    save("/tmp/edgesHH.wkt", 
      hh.map{ hedge =>
        val wkt = hedge.edge.toText

        s"$wkt\t${hedge.tags}\t${hedge.updateTags}\t${hedge.prev}\n"
      }.toList
    )
    /* Debug */

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
