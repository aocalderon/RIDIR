package edu.ucr.dblab.debug

import scala.collection.mutable.{PriorityQueue}
import scala.collection.JavaConverters._

import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory, Coordinate, Point}
import com.vividsolutions.jts.geomgraph.index.SimpleMCSweepLineIntersector
import com.vividsolutions.jts.geomgraph.index.SegmentIntersector
import com.vividsolutions.jts.algorithm.RobustLineIntersector
import com.vividsolutions.jts.geomgraph.EdgeIntersection
import com.vividsolutions.jts.geomgraph.Edge

import edu.ucr.dblab.bo.BentleyOttmann
import edu.ucr.dblab.sdcel.geometries.{Half_edge, HEdge, Seg}
import edu.ucr.dblab.sdcel.geometries.{EventPoint, EventPoint_Ordering, CoordYX_Ordering}
import edu.ucr.dblab.sdcel.geometries.{LEFT_ENDPOINT, INTERSECT, RIGHT_ENDPOINT}
import edu.ucr.dblab.sdcel.Utils.{save, logger}

object BO {
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

    val hedges = method match {
      case "Dummy"  => generateHedges
      case "Random" => generateRandomHedges(n)
      case "File"   => generateFromFile(file1)
      case _ => generateHedges
    }

    if(debug){
      save("/tmp/edgesH.wkt"){
        hedges.map{ d => s"${d.wkt}\t${d.tag}\n" }
      }
    }

    val inters1 = sweepline(hedges, debug)
    val inters2 = sweeplineJTS(hedges)

    val n1 = inters1.size
    val n2 = inters2.size
    println(s"Number of intersections: Is $n1 == $n2 ? ${n1 == n2}")

    val I1 = inters1.sortBy{i => (i.getX, i.getY)}
    val I2 = inters2.sortBy{i => (i.getX, i.getY)}
    val sample = 10

    val stats = (I1 zip I2)
      .map{ case(i1, i2) =>
        (i1, i2, i1.distance(i2))
      }
    stats.take(10).foreach{ case(i1, i2, d) =>
      println{s"${i1.toText}\t${i2.toText}\t${d}"}
    }
    val S = stats.map(_._3)
    val max = S.max
    val sum = S.sum
    val len = S.length
    val avg = sum / len
    println(s"Avg: ${avg} Max: ${max}")

    logger.info(s"INFO|${runId}|${n1}|${n2}|${max}|${sum}|${len}|${avg}")
  }

  def sweepline(hedges: List[Half_edge], debug: Boolean = false)
    (implicit geofactory: GeometryFactory): List[Point] = {

    val event_points = hedges.zipWithIndex.map{ case(h, id) =>
      val left  = EventPoint(List(h), LEFT_ENDPOINT,  id)
      val right = EventPoint(List(h), RIGHT_ENDPOINT, id)
      List( left, right )
    }.flatten

    implicit val scheduler: PriorityQueue[EventPoint] =
      PriorityQueue( event_points: _* )(EventPoint_Ordering)

    if(debug){
      save("/tmp/edgesScheduler.wkt"){
        scheduler.clone.dequeueAll.zipWithIndex.map{ case(e, t) =>
          val x = e.getEventPoint.x
          val y = e.getEventPoint.y
          val h = e.head
          s"${h.wkt}\t${t}\t${h.tag}\t${e.id}\t${x}\t${y}\t${e.event}\n"
        }
      }
      save("/tmp/edgesStatus.wkt"){
        val sweeps = scheduler.clone.dequeueAll.zipWithIndex.map{ case(e, t) =>
          val x = e.getEventPoint.x
          val c = Array(new Coordinate(x, 0), new Coordinate(x,1000))
          val sweep = geofactory.createLineString(c)
          sweep.setUserData(t)
          sweep
        }

        val _join = for {
          sweep <- sweeps
          hedge <- hedges if (sweep.intersects(hedge.edge))
            } yield {
          val inter = sweep.intersection(hedge.edge).getCoordinates.head
          (sweep.toText, hedge.tag, inter, sweep.getUserData.asInstanceOf[Int])
        }
        val r = _join.groupBy(_._1).map{ case(key, values) =>
          (s"""${key}\t${values.sortBy(_._3.y).map(_._2).mkString(" ")}\n""", values.map(_._4).head)
        }.toList.sortBy(_._2).map(_._1)

        r
      }
    }

    val edges = hedges.map{ hedge =>
      Seg(hedge).edge
    }
    val intersector = new BentleyOttmann(edges.asJava)
    val intersections = intersector.get_intersections.asScala.zipWithIndex
      .map{ case (intersect, id)  =>
        val i = geofactory.createPoint(intersect.getPoint.asJTSCoordinate())
        i.setUserData(id)
        i
      }.toList

    if(debug){
      save("/tmp/edgesI.wkt"){
        intersections.map{ case intersect  =>
          val wkt = intersect.toText
          val id  = intersect.getUserData.asInstanceOf[Int]
          s"$wkt\t$id\n"
        }
      }
    }

    intersections
  }

  def sweeplineJTS(hedges: List[Half_edge])(implicit geofactory: GeometryFactory): List[Point] = {
    val aList = hedges.map{ h =>
      val pts = Array(h.v1, h.v2)
      HEdge(pts, h)
    }.asJava
    
    val sweepline = new SimpleMCSweepLineIntersector()
    val lineIntersector = new RobustLineIntersector()
    val segmentIntersector = new SegmentIntersector(lineIntersector, true, true)
    sweepline.computeIntersections(aList, segmentIntersector, false)

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

    aPoints
  }  
  
  def generateFromFile(filename: String)(implicit geofactory: GeometryFactory): List[Half_edge] = {
    import scala.io.Source
    import com.vividsolutions.jts.io.WKTReader
    import com.vividsolutions.jts.geom.LineString

    val reader = new WKTReader(geofactory)
    val buffer = Source.fromFile(filename)
    val hedges = buffer.getLines.map{ line =>
      val arr = line.split("\t")
      val linestring = reader.read(arr(0)).asInstanceOf[LineString]
      val hedge = Half_edge(linestring)
      hedge.id  = arr(1).toInt
      hedge
    }.toList
    buffer.close

    hedges
  }

  def generateRandomHedges(n: Int, label: String = "A", factor: Double = 1000.0)
    (implicit geofactory: GeometryFactory): List[Half_edge] = {

    import scala.util.Random

    (0 until n).map{ x =>
      val v1 = new Coordinate(Random.nextDouble() * factor, Random.nextDouble() * factor)
      val v2 = new Coordinate(Random.nextDouble() * factor, Random.nextDouble() * factor)
      val l  = geofactory.createLineString(Array(v1, v2))
      val h  = Half_edge(l)
      h.tag = label
      h.id  = x
      h
    }.toList
  }

  def generateHedges(implicit geofactory: GeometryFactory): List[Half_edge] = {
    val a = geofactory.createLineString(Array(new Coordinate(1,1), new Coordinate(3,2)))
    val b = geofactory.createLineString(Array(new Coordinate(1,3), new Coordinate(5,5)))
    val c = geofactory.createLineString(Array(new Coordinate(1,6), new Coordinate(3,5)))
    val d = geofactory.createLineString(Array(new Coordinate(1,7), new Coordinate(3,8)))
    val e = geofactory.createLineString(Array(new Coordinate(1,9), new Coordinate(3,7)))
    val f = geofactory.createLineString(Array(new Coordinate(2,3), new Coordinate(4,2)))
    val g = geofactory.createLineString(Array(new Coordinate(2,5), new Coordinate(4,7)))
    val h = geofactory.createLineString(Array(new Coordinate(2,9), new Coordinate(5,9)))
    val i = geofactory.createLineString(Array(new Coordinate(3,1), new Coordinate(5,2)))
    val j = geofactory.createLineString(Array(new Coordinate(3,3), new Coordinate(5,7)))
    val k = geofactory.createLineString(Array(new Coordinate(4,1), new Coordinate(5,1)))

    List(a,b,c,d,e,f,g,h,i,j,k)
      .zip(List("A","B","C","D","E","F","G","H","I","J","K"))
      .map{ case(line, tag) => val h = Half_edge(line); h.tag = tag; h }
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

  verify()
}

