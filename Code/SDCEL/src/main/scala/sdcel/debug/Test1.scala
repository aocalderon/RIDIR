package edu.ucr.dblab.debug

import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory, Coordinate}
import edu.ucr.dblab.sdcel.geometries.{Half_edge, Cell, SL_Hedge, StatusKey}
import edu.ucr.dblab.sdcel.geometries.{EventPoint, EventPoint_Ordering, CoordYX_Ordering}
import edu.ucr.dblab.sdcel.geometries.{Event, LEFT_ENDPOINT, INTERSECTION, RIGHT_ENDPOINT}
import edu.ucr.dblab.sdcel.Utils.save
import com.google.common.collect.TreeMultiset
import scala.collection.mutable.{PriorityQueue}
import scala.collection.JavaConverters._
import java.util.TreeMap

object BSTreeTest{
  def main(args: Array[String]) = {
    implicit val geofactory = new GeometryFactory(new PrecisionModel(1000.0))
    implicit val cells = Map(0 -> getCell)

    val hedges = generateHedges
    save("/tmp/edgesH.wkt"){
      hedges.map{ d => s"${d.wkt}\t${d.tag}\n" }
    }

    val event_points = hedges.zipWithIndex.map{ case(h, id) =>
      val left  = EventPoint(List(h), LEFT_ENDPOINT,  id)
      val right = EventPoint(List(h), RIGHT_ENDPOINT, id)
      List( left, right )
    }.flatten
    val scheduler: PriorityQueue[EventPoint] =
      PriorityQueue( event_points: _* )(EventPoint_Ordering)
    save("/tmp/edgesE.wkt"){
      scheduler.clone.dequeueAll.zipWithIndex.map{ case(e, t) =>
        val x = e.getEventPoint.x
        val y = e.getEventPoint.y
        val h = e.head
        s"${h.wkt}\t${t}\t${h.tag}\t${e.id}\t${x}\t${y}\t${e.event}\n"
      }
    }
    
    val status: AVLTreeST[StatusKey, StatusKey] = new AVLTreeST[StatusKey, StatusKey]()
    val alpha: Queue[(StatusKey, StatusKey)] = new Queue() 

    while(!scheduler.isEmpty){

      val point = scheduler.dequeue
      val tag = point.event match {
        case LEFT_ENDPOINT => {
          val s = StatusKey(point.head, point.head.left)
          status.put(s, s)
          val s1 = s.above(status)
          val s2 = s.below(status)
          if( s.intersects(s1) ) alpha.enqueue( (s1.get, s) ) 
          if( s.intersects(s2) ) alpha.enqueue( ( s,s2.get) )
          s"PUT $s"
        }
        case RIGHT_ENDPOINT => {
          val s = StatusKey(point.head, point.head.left)
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
          val s1_prime = StatusKey(h1, h1.left)
          val s2_prime = StatusKey(h2, h2.left)

          val (s1, s2) = if(StatusKey.above(s1_prime, s2_prime.left, s2_prime.right))
            (s2_prime, s1_prime)
          else
            (s1_prime, s2_prime)

          val s3 = StatusKey.above(status, s1)
          val s4 = StatusKey.below(status, s2)

          if( s2.intersects(s3) ) alpha.enqueue( (s2, s3.get) )
          if( s1.intersects(s4) ) alpha.enqueue( (s1, s4.get) ) 

          val temp = status.get(s1)
          status.put(s1, s2)
          status.put(s2, temp)

          s"INT $s1 $s2"
        }
      }

      val inters = new StringBuffer()
      while(!alpha.isEmpty){
        val (s1, s2) = alpha.dequeue
        val intersection = s1.intersection(s2)
        val point = EventPoint(List(s1.hedge, s2.hedge), INTERSECTION,  -1, intersection)
        if(scheduler.exists(_ == point) == false){
          val wkt = geofactory.createPoint(point.intersection).toText()
          scheduler.enqueue(point)
          inters.append(s" (${point.head.tag}, ${point.last.tag}, ${wkt})")
        }
      }

      printStatus(status, point, tag, inters)
    }
  }

  def printStatus(status: AVLTreeST[StatusKey, StatusKey], point: EventPoint, tag: String,
    inters: StringBuffer): Unit = {

    val p = point.getEventPoint
    val coords = s"(${p.x}, ${p.y})"
    val out = status.keys().asScala.map{ key =>
      val value = status.get(key)
      value.hedge.tag
    }.mkString(" ")

    println(f"${point.event}%15s ${tag}%10s ${coords}%15s [${out}%25s] ${inters.toString}")
  }

  def getCell(implicit geofactory: GeometryFactory): Cell = {
    val a = new Coordinate( 0, 0)
    val b = new Coordinate(10, 0)
    val c = new Coordinate(10,10)
    val d = new Coordinate( 0,10)
    Cell(0, "0", geofactory.createLinearRing(Array(a,b,c,d,a)))
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

