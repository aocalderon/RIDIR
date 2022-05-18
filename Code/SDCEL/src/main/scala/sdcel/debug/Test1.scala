package edu.ucr.dblab.debug

import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory, Coordinate, Point}
import edu.ucr.dblab.sdcel.geometries.{Half_edge, StatusKey, StatusValue, Sweep, Seg}
import edu.ucr.dblab.sdcel.geometries.{EventPoint, EventPoint_Ordering, CoordYX_Ordering}
import edu.ucr.dblab.sdcel.geometries.{Event, LEFT_ENDPOINT, INTERSECTION, RIGHT_ENDPOINT}
import edu.ucr.dblab.sdcel.Utils.save
import scala.collection.mutable.{PriorityQueue, ListBuffer}
import scala.collection.JavaConverters._

object BSTreeTest{
  def main(args: Array[String]) = {
    implicit val geofactory: GeometryFactory = new GeometryFactory(new PrecisionModel(1000.0))
    
    val debug: Boolean = true
    val method: String = "Random"
    val filename: String = "/home/acald013/tmp/edgesH.wkt"
    val n: Int = 250

    val hedges = method match {
      case "Dummy"  => generateHedges
      case "Random" => generateRandomHedges(n)
      case "File"   => generateFromFile(filename)
      case _ => generateHedges
    }

    if(debug){
      save("/tmp/edgesH.wkt"){
        hedges.map{ d => s"${d.wkt}\t${d.tag}\n" }
      }
    }

    sweepline(hedges, debug).foreach{println}
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
    import edu.ucr.dblab.bo.BentleyOttmann
    val intersector = new BentleyOttmann(edges.asJava)
    save("/tmp/edgesI.wkt"){
      intersector.get_intersections.asScala.zipWithIndex.map{ case (intersect, id)  =>
        val wkt = geofactory.createPoint(intersect.asJTSCoordinate()).toText
        s"$wkt\t$id\n"
      }
    }

    implicit val status: AVLTreeST[StatusKey, StatusValue] = new AVLTreeST[StatusKey, StatusValue]()
    val alpha: Queue[(StatusValue, StatusValue)] = new Queue() 
    var I = new ListBuffer[Point]()

    val n = hedges.size
    val max = n * (n - 1) / 2.0
    var j = 0
    while(!scheduler.isEmpty && j < max){
      val point = scheduler.dequeue
      implicit val sweep: Sweep = Sweep(point.getEventPoint.x)
      val tag = point.event match {
        case LEFT_ENDPOINT => {
          val h = point.head
          val s = StatusKey(h.left, h.right, h.tag)
          val value = StatusValue(s, h)
          status.put(s, value)
          val s1 = s.above(status)
          val s2 = s.below(status)
          if( s.intersects(s1) ) alpha.enqueue( (s1.get, value) ) 
          if( s.intersects(s2) ) alpha.enqueue( (s2.get, value) )
          s"PUT $s"
        }
        case RIGHT_ENDPOINT => {
          val h = point.head
          val s = StatusKey(h.left, h.right, h.tag)
          val s1 = s.above(status)
          val s2 = s.below(status)
          val p = point.getEventPoint
          if(StatusKey.intersection(s1, s2, p)) alpha.enqueue( (s1.get, s2.get) ) 
          status.delete(s)
          s"DEL $s"
        }
        case INTERSECTION => {
          val hedges = point.hedges
          val p = point.getEventPoint
          val h1 = hedges(0)
          val h2 = hedges(1)
          val s1_prime = StatusKey(h1.left, h1.right, h1.tag)
          val s2_prime = StatusKey(h2.left, h2.right, h2.tag)

          val (s1, s2) = if(StatusKey.above(s1_prime, s2_prime.left, s2_prime.right))
            (status.get(s2_prime), status.get(s1_prime))
          else
            (status.get(s1_prime), status.get(s2_prime))

          if(debug){
            println(s"s1: ${s1}")
            println(s"s2: ${s2}")
          }

          val s3 = StatusKey.above(s1)
          val s4 = StatusKey.below(s2)

          if(debug){
            println(s"s3: ${s3}")
            println(s"s4: ${s4}")
          }

          if( s2.key.intersects(s3) ){
            if(debug) println(s"Intersection between s2($s2) and s3($s3)")
            alpha.enqueue( (s2, s3.get) )
          }
          if( s1.key.intersects(s4) ){
            if(debug) println(s"Intersection between s1($s1) and s4($s4)")
            alpha.enqueue( (s1, s4.get) )
          }

          val s1Value = s1
          println(s"s1Value: $s1Value")
          val s2Value = s2
          println(s"s2Value: $s2Value")
          status.delete(s1_prime)
          printStatus(point, s"DEL $s1_prime")
          status.delete(s2_prime)
          printStatus(point, s"DEL $s2_prime")
          status.put(s1.key, s1Value)
          printStatus(point, s"PUT $s1Value")
          status.put(s2.key, s2Value)
          printStatus(point, s"PUT $s2Value")

          s"INT $s1 $s2"
        }
      }

      val inters = new StringBuffer()
      while(!alpha.isEmpty){
        val (s1, s2) = alpha.dequeue
        val intersection = s1.key.intersection(s2)
        val point = EventPoint(List(s1.hedge, s2.hedge), INTERSECTION,  -1, intersection)
        if(scheduler.exists(_ == point) == false){
          val i = geofactory.createPoint(point.intersection)
          I.append(i)
          //scheduler.enqueue(point)
          inters.append(s" (${point.head.tag}, ${point.last.tag}, ${i.toText})")
        }
      }

      if(debug) printStatus(point, tag, inters)
      j = j + 1

      if(point.getEventPoint.x == 205.81 && point.getEventPoint.y == 211.005)
        System.exit(0)
    }

    I.toList.distinct
  }

  def printStatus(point: EventPoint, tag: String, inters: StringBuffer = new StringBuffer())
      (implicit status: AVLTreeST[StatusKey, StatusValue], geofactory: GeometryFactory): Unit = {

    val p = point.getEventPoint
    val coords = s"(${p.x}, ${p.y})"
    val out = status.keys().asScala.map{ key =>
      val value = status.get(key)
      value.hedge.tag
    }.mkString(" ")

    println(f"${point.event}%15s ${tag}%10s ${coords}%15s [${out}] ${inters.toString}")
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
      hedge.tag = arr(1)
      hedge
    }.toList
    buffer.close

    hedges
  }

  def generateRandomHedges(n: Int, factor: Double = 1000.0)
    (implicit geofactory: GeometryFactory): List[Half_edge] = {

    import scala.util.Random

    (0 to n).map{ x =>
      val v1 = new Coordinate(Random.nextDouble() * factor, Random.nextDouble() * factor)
      val v2 = new Coordinate(Random.nextDouble() * factor, Random.nextDouble() * factor)
      val l  = geofactory.createLineString(Array(v1, v2))
      val h  = Half_edge(l)
      h.tag = x.toString
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

