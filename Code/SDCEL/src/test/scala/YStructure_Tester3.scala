import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, PrecisionModel, LineString}
import com.vividsolutions.jts.io.WKTReader
import edu.ucr.dblab.sdcel.Utils.save
import edu.ucr.dblab.sdcel.geometries.Half_edge
import org.scalatest.flatspec._
import org.scalatest.matchers._
import sdcel.bo._

import java.util.TreeMap
import scala.collection.JavaConverters._
import scala.util.Random

class YStructure_Tester3 extends AnyFlatSpec with should.Matchers {
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

  def generateSegments(n: Int, label: String = "*")(implicit geofactory: GeometryFactory): List[Segment] = {
    val yRange = 100.0
    val xRange = 100.0
    (0 to n).map{ i =>
      val y1 = Random.nextDouble() * yRange
      val y2 = Random.nextDouble() * yRange
      val p1 = new Coordinate(   0.0, y1)
      val p2 = new Coordinate(xRange, y2)
      val l = geofactory.createLineString(Array(p1,p2))
      val h = Half_edge(l); h.id = i
      Segment(h, label)
    }.toList
  }

  def readSegments(filename: String)(implicit geofactory: GeometryFactory): List[Segment] = {
    import scala.io.Source
    val reader = new WKTReader(geofactory)
    val buffer = Source.fromFile(filename)
    buffer.getLines().map { line =>
      val arr = line.split("\t")
      val wkt = arr(0)
      val lab = arr(1).substring(0,1)
      val id  = arr(3).toInt
      val l: LineString = reader.read(wkt).asInstanceOf[LineString]
      val h = Half_edge(l); h.id = id
      Segment(h, lab)
    }.toList
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

  val segs_training = generateSegments(20)
  val segs_testing  = generateSegments(10)

  save("/tmp/edgesST1.wkt"){
    segs_training.map{ seg =>
      val wkt = seg.wkt
      val id  = seg.id
      s"$wkt\t$id\n"
    }
  }
  save("/tmp/edgesST2.wkt") {
    segs_testing.map { seg =>
      val wkt = seg.wkt
      val id = seg.id
      s"$wkt\t$id\n"
    }
  }
  val (lower_sentinel, upper_sentinel) = BentleyOttmann.getSentinels(segs_training ++ segs_testing)

  val cmp = new sweep_cmp()
  cmp.setSweep(lower_sentinel.source)

  implicit val Y_structure: TreeMap[Segment, Seq_item] = new TreeMap[Segment, Seq_item](cmp)

  Y_structure.put(upper_sentinel, Seq_item(Key(upper_sentinel), null))
  Y_structure.put(lower_sentinel, Seq_item(Key(lower_sentinel), null))

  segs_training.map{ s =>
    Y_structure_add(s)
  }

  println(Y_structure_content(Y_structure))

  println("Done!")
}
