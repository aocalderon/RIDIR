import org.scalatest._
import flatspec._
import matchers._

import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory, Coordinate}
import edu.ucr.dblab.bo3.{BentleyOttmann, Segment, Seq_item, sweep_cmp, sweep_cmp2, sweep_cmp3}
import edu.ucr.dblab.sdcel.geometries.Half_edge
import edu.ucr.dblab.sdcel.Utils.{save, logger}

import java.util.TreeMap
import scala.collection.JavaConverters._

class SortSeq_Tester extends AnyFlatSpec with should.Matchers {
  val debug: Boolean = true
  implicit val model: PrecisionModel = new PrecisionModel(1000.0)
  implicit val geofactory: GeometryFactory = new GeometryFactory(model)

  ////////////////////////////////// Basic Functionality Tests////////////////////////////////

  val p0  = new Coordinate(2, 8); val p1  = new Coordinate(5, 1) // a
  val p2  = new Coordinate(3, 1); val p3  = new Coordinate(5, 3) // b
  val p4  = new Coordinate(2, 1); val p5  = new Coordinate(5, 4) // c
  val p6  = new Coordinate(3,10); val p7  = new Coordinate(5, 5) // d
  val p8  = new Coordinate(2, 3); val p9  = new Coordinate(5, 6) // e
  val p10 = new Coordinate(2, 5); val p11 = new Coordinate(5, 7) // f
  val p12 = new Coordinate(2, 9); val p13 = new Coordinate(5, 8) // g
  val p14 = new Coordinate(2, 7); val p15 = new Coordinate(5,10) // h
  val l1 = geofactory.createLineString( Array(p0,  p1)  )
  val l2 = geofactory.createLineString( Array(p2,  p3)  )
  val l3 = geofactory.createLineString( Array(p4,  p5)  )
  val l4 = geofactory.createLineString( Array(p6,  p7)  )
  val l5 = geofactory.createLineString( Array(p8,  p9)  )
  val l6 = geofactory.createLineString( Array(p10, p11) )
  val l7 = geofactory.createLineString( Array(p12, p13) )
  val l8 = geofactory.createLineString( Array(p14, p15) )
  val h1 = Half_edge(l1); h1.id = 1
  val h2 = Half_edge(l2); h2.id = 2
  val h3 = Half_edge(l3); h3.id = 3
  val h4 = Half_edge(l4); h4.id = 4
  val h5 = Half_edge(l5); h5.id = 5
  val h6 = Half_edge(l6); h6.id = 6
  val h7 = Half_edge(l7); h7.id = 7
  val h8 = Half_edge(l8); h8.id = 8
  val hh = List(h3, h5, h6, h8, h1, h7)
  val segs = hh.map{ h => Segment(h, "A") }

  if(debug){
    save("/tmp/edgesSSsegs.wkt"){
      segs.map{ seg =>
        val wkt = seg.wkt
        val id  = seg.id

        s"$wkt\t$id\n"
      }
    }
  }

  // Setting lower and upper sentinels to bound the algorithm...
  val (lower_sentinel, upper_sentinel) = BentleyOttmann.getSentinels
  var p_sweep = new Coordinate(2.0, 1.0)

  // Setting the order criteria for Y-Structure
  val cmp = new sweep_cmp()
  cmp.setPosition(p_sweep)

  val cmp2 = new sweep_cmp2()
  cmp2.setSweep(p_sweep)

  val cmp3 = new sweep_cmp3()
  cmp3.setY(p_sweep.y)

  // The Y-Structure: Sweep line status...
  case class T(key: Segment, value: String)
  implicit val Y_structure: TreeMap[Segment, T] = new TreeMap[Segment, T](cmp3)

  segs.foreach{ seg =>
    seg.sweep = p_sweep
    val o = Y_structure.put(seg, T(seg, seg.id.toString))
    println(o)
  }

  Y_structure.asScala.keySet.map{ k => s"${k.wkt}\t${Y_structure.get(k)}" }.foreach(println)

  if(debug){
    save("/tmp/edgesSSstatus.wkt"){
      Y_structure.values.asScala.toList.map{ t =>
        val wkt = t.key.wkt
        val id  = t.value
        s"$wkt\t$id\n"
      }
    }
  }



  /*
   "Points p, q, r1 " should " be left oriented" in {
   BentleyOttmann.orientation(p, q, r1) should be  (1)
   }
   "Points p, q, r2 " should " be collinear" in {
   BentleyOttmann.orientation(p, q, r2) should be  (0)
   }
   "Points p, q, r3 " should " be right oriented" in {
   BentleyOttmann.orientation(p, q, r3) should be (-1)
   }
   */
}
