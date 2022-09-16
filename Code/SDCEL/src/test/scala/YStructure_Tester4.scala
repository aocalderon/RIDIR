import com.vividsolutions.jts.geom.{Coordinate, Envelope, GeometryFactory, LineString, PrecisionModel}
import com.vividsolutions.jts.io.WKTReader
import edu.ucr.dblab.sdcel.Utils.save
import edu.ucr.dblab.sdcel.geometries.Half_edge
import org.scalatest.flatspec._
import org.scalatest.matchers._
import sdcel.bo._

import java.util.TreeMap
import scala.collection.JavaConverters._
import scala.util.Random

class YStructure_Tester4 extends AnyFlatSpec with should.Matchers {
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

  def orderPoints(x1: Double, y1: Double, x2: Double, y2: Double): Array[Coordinate] = {
    val (a, b) = if(x1 < x2){ (x1, x2) } else { (x2, x1) }
    val (c, d) = if(y1 < y2){ (y1, y2) } else { (y2, y1) }
    val p = new Coordinate(a, c)
    val q = new Coordinate(b, d)
    Array(p, q)
  }

  def generateSegments(n: Int, yRange: Double = 100.0, xRange: Double = 100.0, label: String = "*")
                      (implicit geofactory: GeometryFactory): List[Segment] = {
    (0 to n).map{ i =>
      val x1 = Random.nextDouble() * xRange
      val y1 = Random.nextDouble() * yRange
      val x2 = Random.nextDouble() * xRange
      val y2 = Random.nextDouble() * yRange
      val points = orderPoints(x1,y1,x2,y2)
      val l = geofactory.createLineString(points)
      val h = Half_edge(l); h.id = i
      Segment(h, label)
    }.toList
  }

  def generateSegmentsEnv(env: Envelope, n: Int , label: String = "*")
                      (implicit geofactory: GeometryFactory): List[Segment] = {
    val a = env.getMinX; val b = env.getMinY;
    val c = env.getMaxX; val d = env.getMaxY;
    val xRange = Math.abs(c - a)
    val yRange = Math.abs(b - d)
    (0 to n).map { i =>
      val x1 = a + ( Random.nextDouble() * xRange )
      val y1 = b + ( Random.nextDouble() * yRange )
      val x2 = a + ( Random.nextDouble() * xRange )
      val y2 = b + ( Random.nextDouble() * yRange )
      val points = orderPoints(x1, y1, x2, y2)
      val l = geofactory.createLineString(points)
      val h = Half_edge(l);
      h.id = i
      Segment(h, label)
    }.toList
  }
  def readSegments(filename: String)(implicit geofactory: GeometryFactory): List[Segment] = {
    import scala.io.Source
    val reader = new WKTReader(geofactory)
    val buffer = Source.fromFile(filename)
    val segs = buffer.getLines().map { line =>
      val arr = line.split("\t")
      val wkt = arr(0)
      val lab = arr(1).substring(0,1)
      val id  = arr(3).toInt
      val l: LineString = reader.read(wkt).asInstanceOf[LineString]
      val h = Half_edge(l); h.id = id
      Segment(h, lab)
    }.toList
    buffer.close()
    segs
  }

  def generateIntervals(env: Envelope, n: Int, stri: String, label: String = "+"): Array[ List[Segment] ] = {
    val y1 = env.getMinY
    val y2 = env.getMaxY

    stri.split(",").zipWithIndex.map{ case (interval, i) =>
      val arr = interval.split(" ")
      val x1 = arr(0).toDouble
      val x2 = arr(1).toDouble
      val env = new Envelope(x1, x2, y1, y2)
      generateSegmentsEnv(env, n, s"${label}$i")
    }
  }

  val debug: Boolean = true
  val tolerance: Double = 1e-3
  val generate: Boolean = false
  implicit val model: PrecisionModel = new PrecisionModel(1000.0)
  implicit val geofactory: GeometryFactory = new GeometryFactory(model)

  implicit val settings: Settings = Settings(
    debug = debug,
    tolerance = tolerance,
    geofactory = geofactory
  )

  val big_dataset = if(generate) {
    val bd = generateSegments(n = 100, xRange = 1000.0, yRange = 250.0, label = "A")
    save("/tmp/edgesBD.wkt") {
      bd.map { seg =>
        val wkt = seg.wkt
        val id = seg.id
        s"$wkt\t$id\n"
      }
    }
    bd
  } else {
    readSegments(filename = "/tmp/edgesBD.wkt")
  }

  val envelope = new Envelope(0, 1000, 0, 250)
  val stri = "200 300,400 500,750 900"
  val intervals = generateIntervals(envelope, 10, stri, label = "B")

  intervals.zipWithIndex.foreach{ case (segments, i) =>
    save(s"/tmp/edgesSD${i}.wkt"){
      segments.map{ segment =>
        val wkt = segment.wkt
        val  id = segment.id
        s"$wkt\t$id\n"
      }
    }
  }

  val (lower_sentinel, upper_sentinel) = BentleyOttmann.getSentinels(big_dataset)

  val cmp = new sweep_cmp()
  cmp.setSweep(lower_sentinel.source)

  implicit val Y_structure: TreeMap[Segment, Seq_item] = new TreeMap[Segment, Seq_item](cmp)

  println("Done!")
}
