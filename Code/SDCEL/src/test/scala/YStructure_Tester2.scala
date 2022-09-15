import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel}
import sdcel.bo._
import org.scalatest.flatspec._
import org.scalatest.matchers._
import edu.ucr.dblab.sdcel.Utils.save

import java.util.TreeMap
import scala.collection.JavaConverters._

class YStructure_Tester2 extends AnyFlatSpec with should.Matchers {
  def Y_structure_add(s: Segment)(implicit Y: TreeMap[Segment, Seq_item]): Unit = {
    cmp.setSweep(s.source)
    Y_structure.put(s, null)
  }

  def Y_structure_content(Y: TreeMap[Segment, Seq_item]): String = {
    Y.asScala.iterator.filter { case (s, i) => s.id >= 0 }.map { case (s, i) => s.id }.mkString(" ")
  }

  def Y_structure_test(number: Int)(implicit Y: TreeMap[Segment, Seq_item]): Unit = {
    val r = Y_structure_content(Y)
    println(r)
    s"Y_structure in $number" should s"be $r" in {
      ground_truth(number) should be(r)
    }
  }

  val ground_truth: Map[Int, String] = Map(
    1 -> "1 2 3",
    2 -> "1 4 2",
    3 -> "1 5 4 2",
    4 -> "1 6"
  )

  val debug: Boolean = true
  val tolerance: Double = 1e-3
  implicit val model: PrecisionModel = new PrecisionModel(1000.0)
  implicit val geofactory: GeometryFactory = new GeometryFactory(model)

  implicit val settings: Settings = Settings(
    debug = debug,
    tolerance = tolerance,
    geofactory = geofactory
  )

  val (_, segs) = BentleyOttmann.loadData2
  save("/tmp/edgesS.wkt"){
    segs.map{ seg =>
      val wkt = seg.wkt
      val id  = seg.id
      s"$wkt\t$id\n"
    }
  }
  val (s2, s3, s5, s6, s8, s1, s7, s4) = (segs(0), segs(1), segs(2), segs(3), segs(4), segs(5), segs(6), segs(7))

  val (lower_sentinel, upper_sentinel) = BentleyOttmann.getSentinels(segs)

  val cmp = new sweep_cmp()
  cmp.setSweep(lower_sentinel.source)

  implicit val Y_structure: TreeMap[Segment, Seq_item] = new TreeMap[Segment, Seq_item](cmp)

  Y_structure.put(upper_sentinel, Seq_item(Key(upper_sentinel), null))
  Y_structure.put(lower_sentinel, Seq_item(Key(lower_sentinel), null))

  Y_structure_add(s3)

  Y_structure_add(s5)

  Y_structure_add(s6)

  Y_structure_add(s8)

  Y_structure_add(s1)

  Y_structure_add(s7)

  Y_structure_add(s2)

  Y_structure_add(s4)

  println("Done!")
}
