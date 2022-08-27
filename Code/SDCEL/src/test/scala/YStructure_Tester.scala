import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, LineString, PrecisionModel}
import org.jgrapht.graph.SimpleDirectedGraph
import sdcel.bo._
import org.scalatest.flatspec._
import org.scalatest.matchers._

import java.util.TreeMap
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class YStructure_Tester extends AnyFlatSpec with should.Matchers {
  def Y_structure_content(Y: TreeMap[Segment, Seq_item]): String = {
    Y.asScala.iterator.filter{ case(s, i) => s.id >= 0}.map{ case(s, i) => s.id }.mkString(" ")
  }

  val ground_truth: Map[Int, String] = Map(
    1 -> "1 2 3",
    2 -> "1 4 2",
    3 -> "1 5 4 2",
    4 -> "1 6"
  )

  def Y_structure_test(number: Int)(implicit Y: TreeMap[Segment, Seq_item]): Unit = {
    val r = Y_structure_content(Y)
    println(r)
    s"Y_structure in $number" should s"be $r" in { ground_truth(number) should be(r) }
  }

  val debug: Boolean = true
  val tolerance: Double = 1e-3
  implicit val model: PrecisionModel = new PrecisionModel(1000.0)
  implicit val geofactory: GeometryFactory = new GeometryFactory(model)

  implicit val settings: Settings = Settings(
    debug = debug,
    tolerance = tolerance,
    geofactory = geofactory
  )

  val (_, segs) = BentleyOttmann.loadData3
  val (s1,s2,s3,s4,s5,s6) = (segs(0), segs(1),segs(2),segs(3).reverse,segs(4),segs(5).reverse)

  val (lower_sentinel, upper_sentinel) = BentleyOttmann.getSentinels(segs)

  // Setting the order criteria for Y-Structure
  val cmp = new sweep_cmp()
  cmp.setSweep(lower_sentinel.source)

  implicit val Y_structure: TreeMap[Segment, Seq_item] = new TreeMap[Segment, Seq_item](cmp)

  Y_structure.put( upper_sentinel, Seq_item( Key(upper_sentinel), null) )
  Y_structure.put( lower_sentinel, Seq_item( Key(lower_sentinel), null) )

  cmp.setSweep(s1.source)
  Y_structure.put(s1, null)

  cmp.setSweep(s2.source)
  Y_structure.put(s2, null)

  cmp.setSweep(s3.source)
  Y_structure.put(s3, null)

  Y_structure_test(1)

  Y_structure.remove(s3)
  cmp.setSweep(s4.source)
  Y_structure.put(s4, null)

  Y_structure_test(2)

  cmp.setSweep(s5.source)
  Y_structure.put(s5, null)

  Y_structure_test(3)

  Y_structure.remove(s2)
  Y_structure.remove(s4)
  Y_structure.remove(s5)
  cmp.setSweep(s6.source)
  Y_structure.put(s6, null)

  Y_structure_test(4)
}


