import collection.mutable.Stack
import org.scalatest._
import flatspec._
import matchers._

import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory, Coordinate, Point}
import edu.ucr.dblab.bo3.{BentleyOttmann, Segment, Event, Tree, Node}
import edu.ucr.dblab.sdcel.geometries.Half_edge
import edu.ucr.dblab.sdcel.Utils.{save, logger}

import java.util.{PriorityQueue, TreeSet}
import scala.collection.mutable.{ListBuffer, ArrayBuffer}
import java.io.PrintWriter
import scala.collection.JavaConverters._

class BO_Tester extends AnyFlatSpec with should.Matchers {
  val debug: Boolean = true
  implicit val model: PrecisionModel = new PrecisionModel(1000.0)
  implicit val geofactory: GeometryFactory = new GeometryFactory(model)
  implicit val sta = ListBuffer[Point]()

  val p0  = new Coordinate(2, 1); val p1  = new Coordinate(7, 4)
  val p2  = new Coordinate(7, 1); val p3  = new Coordinate(1, 4)
  val p4  = new Coordinate(3, 4); val p5  = new Coordinate(5, 6)
  val p6  = new Coordinate(5, 4); val p7  = new Coordinate(3, 6)
  val p8  = new Coordinate(2, 5); val p9  = new Coordinate(7, 8)
  val p10 = new Coordinate(2, 8); val p11 = new Coordinate(5, 8)
  val l1 = geofactory.createLineString(Array(p0,  p1))
  val l2 = geofactory.createLineString(Array(p2,  p3))
  val l3 = geofactory.createLineString(Array(p4,  p5))
  val l4 = geofactory.createLineString(Array(p6,  p7))
  val l5 = geofactory.createLineString(Array(p8,  p9))
  val l6 = geofactory.createLineString(Array(p10, p11))
  val h1 = Half_edge(l1); h1.id = 1
  val h2 = Half_edge(l2); h2.id = 2
  val h3 = Half_edge(l3); h3.id = 3
  val h4 = Half_edge(l4); h4.id = 4
  val h5 = Half_edge(l5); h5.id = 5
  val h6 = Half_edge(l6); h6.id = 6
  val hh1 = List(h1, h3, h5)
  val hh2 = List(h2, h4, h6)
  val segs1 = hh1.map{ h => Segment(h, "A") }
  val segs2 = hh2.map{ h => Segment(h, "B") }
  if(debug){
    save("/tmp/edgesSS1.wkt"){
      segs1.map( e => s"${e.wkt}\t${e.lid}\n" )
    }
    save("/tmp/edgesSS2.wkt"){
      segs2.map( e => s"${e.wkt}\t${e.lid}\n" )
    }
  }
  val segs = segs1 ++ segs2
  implicit var status: TreeSet[Node] = new TreeSet[Node]()

  implicit var scheduler: PriorityQueue[Event] = new PriorityQueue[Event]()
  segs.foreach { seg =>
    scheduler.add( Event( seg.first,  List(seg), 0 ) )
    scheduler.add( Event( seg.second, List(seg), 1 ) )
  }
  val s4 = segs.filter(_.id == 4).head
  val s5 = segs.filter(_.id == 5).head
  scheduler.add( Event( new Coordinate(4.27, 2.36), List(s4, s5), 2 ) )
  val s3 = segs.filter(_.id == 3).head
  scheduler.add( Event( new Coordinate(4, 5), List(s3, s4), 2 ) )
  val s1 = segs.filter(_.id == 1).head
  val s2 = segs.filter(_.id == 2).head
  scheduler.add( Event( new Coordinate(3.25, 5.75), List(s1, s2), 2 ) )

  val event_points = scheduler.iterator.asScala.map(_.point)

  if(debug){
    save("/tmp/edgesSCH.wkt"){
      scheduler.iterator.asScala.toList.map( e => s"${e.wkt}\t${e.value}\t${e.ttype}\n" )
    }
  }

  val segsInT = ArrayBuffer[(Coordinate, Iterator[Long])]()
  while(!scheduler.isEmpty) {
    val event = scheduler.poll()
    println(s"============================ NEW EVENT ============================")
    println(s"EVENT: $event")

    event.ttype match {
      case 0 => {
        val seg = event.segments.head
        status.add( Node( seg.value, ArrayBuffer(seg) ) )
        val Tsegs = Tree.recalculate(event.value)
        //println(s"${event.value}\t${Tsegs.mkString(" ")}")
        
        segsInT.append( (event.point, Tsegs)  )
      }
      case 1 => {
        val seg = event.segments.head
        status.remove( Node( seg.value, ArrayBuffer(seg) ) )
        val Tsegs = Tree.recalculate(event.value)
        //println(s"${event.value}\t${Tsegs.mkString(" ")}")
        
        segsInT.append( (event.point, Tsegs)  )
      }
      case 2 => {
        val seg = event.segments.head
        status.remove( Node( seg.value, ArrayBuffer(seg) ) )
        status.add( Node( seg.value, ArrayBuffer(seg) ) )
        val Tsegs = Tree.recalculate(event.value)
        //println(s"${event.value}\t${Tsegs.mkString(" ")}")
        
        segsInT.append( (event.point, Tsegs)  )
      }
    }
  }

  ////////////////////////////////////////// Orientation //////////////////////////////////////////
  val p  = new Coordinate(2, 5)
  val q  = new Coordinate(8, 7)
  val r1 = new Coordinate(7, 11)
  val r2 = new Coordinate(5, 6)
  val r3 = new Coordinate(1, 1)

  "Points p, q, r1 " should " be left oriented" in {
    BentleyOttmann.orientation(p, q, r1) should be  (1)
  }
  "Points p, q, r2 " should " be collinear" in {
    BentleyOttmann.orientation(p, q, r2) should be  (0)
  }
  "Points p, q, r3 " should " be right oriented" in {
    BentleyOttmann.orientation(p, q, r3) should be (-1)
  }

  ////////////////////////////////////////// Functions //////////////////////////////////////////

  def printStatus(f: PrintWriter, event: Event, filter: String = "*")
    (implicit geofactory: GeometryFactory, status: TreeSet[Node], sta: ListBuffer[Point]) = {

    val points = storeStatusPoint(event)

    val wkt = points.map{ point =>
      val x    = point.getX
      val y    = point.getY
      val sids = point.getUserData.asInstanceOf[Node].segments.map(_.id).mkString(" ")
      s"${point.toText}\t${x}\t${y}\t${sids}\n"
    }.mkString("")

    f.write(wkt)
  }

  def storeStatusPoint(event: Event)(implicit geofactory: GeometryFactory,
    status: TreeSet[Node], sta: ListBuffer[Point]): List[Point] = {

    val points = status.asScala.zipWithIndex.map{ case(node, index)  =>
      val point = geofactory.createPoint(new Coordinate(event.value, node.value))
      node.id = index
      point.setUserData(node)
      point
    }.toList

    sta.appendAll(points)
    points
  }
}
