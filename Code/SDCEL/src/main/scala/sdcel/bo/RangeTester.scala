package sdcel.bo

import com.vividsolutions.jts.geom.{Coordinate, Envelope, GeometryFactory, PrecisionModel}
import com.vividsolutions.jts.index.strtree.STRtree
import edu.ucr.dblab.sdcel.Utils.save
import edu.ucr.dblab.sdcel.geometries.Half_edge

import java.util
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.util.Random
object RangeTester {

  def getEnvelope(segments: List[Segment]): Envelope = {
    segments.map(_.envelope).reduce { (a, b) =>
      a.expandToInclude(b)
      a
    }
  }

  def getX_orderLeftOriented(segments: List[Segment]): List[Segment] = {
    segments.map { segment_prime =>
      // Segments need to be left or upwards oriented...
      if (!segment_prime.isVertical) {
        if (segment_prime.isLeftOriented) {
          segment_prime
        } else {
          segment_prime.reverse
        }
      } else {
        if (segment_prime.isUpwardsOriented) {
          segment_prime
        } else {
          segment_prime.reverse
        }
      }
    }.sortBy(_.source.x)
  }

  def getX_order(segments: List[Segment]): List[Segment] = {
    segments.sortBy(_.source.x)
  }

  def getX_orderBySegment(segments: List[Segment]): util.TreeMap[Coordinate, List[Segment]] = {
    val x_order = new util.TreeMap[Coordinate, List[Segment]]()
    segments.map { segment_prime =>
      // Segments need to be left or upwards oriented...
      if (!segment_prime.isVertical) {
        if (segment_prime.isLeftOriented) {
          segment_prime
        } else {
          segment_prime.reverse
        }
      } else {
        if (segment_prime.isUpwardsOriented) {
          segment_prime
        } else {
          segment_prime.reverse
        }
      }
    }.groupBy(_.source).foreach { case (point, list) =>
      x_order.put(point, list)
    }
    x_order
  }

  def getSubsetsByIndex(big_dataset: List[Segment], intervals: List[Interval]): List[List[Segment]] = {
    // Feeding index...
    val index = new STRtree()
    big_dataset.foreach { segment =>
      index.insert(segment.envelope, segment)
    }

    // Querying index
    val big_envelope = getEnvelope(big_dataset)
    val miny = big_envelope.getMinY
    val maxy = big_envelope.getMaxY
    val subsets = intervals.map { interval =>
      val minx = interval.range.l
      val maxx = interval.range.r
      val envelope = new Envelope(minx, maxx, miny, maxy)
      index.query(envelope).asScala.map(_.asInstanceOf[Segment]).toList
    }

    subsets
  }

  def getSubsetsBySweep2(big_dataset: List[Segment], intervals: List[Interval]): List[List[Segment]] = {
    val x_order = getX_orderBySegment(big_dataset)
    val envelope = getEnvelope(big_dataset)

    // Feeding index...
    val index = new STRtree()
    big_dataset.foreach { segment =>
      index.insert(segment.envelope, segment)
    }

    val subsets = intervals.map { interval =>
      val left = interval.range.l
      val right = interval.range.r

      // Querying the index to get segments intersection left side of the range...
      // These will be segments starting before and entering the current interval...
      val query = new Envelope(left, left, envelope.getMinY, envelope.getMaxY)
      val subset_before = index.query(query).asScala.map(_.asInstanceOf[Segment]).toList

      // Querying the segments inside the interval.  These will be segments starting inside the interval...
      val left_coord = new Coordinate(left, 0.0)
      val right_coord = new Coordinate(right, 0.0)
      val start = x_order.ceilingKey(left_coord)
      val end = x_order.floorKey(right_coord)
      // Extracting segments from x_order between left and right coordinates from the interval...
      val subset_after = x_order.subMap(start, true, end, true)
        .asScala.values.toList.flatten

      // Adding to the final result...
      subset_before ++ subset_after
    }

    subsets
  }

  case class Range(l: Double, r: Double)

  case class Interval(range: Range, subset: List[Segment])

  def extractIntervals2(dataset: List[Segment])(implicit settings: Settings): List[Interval] = {
    log(s"Sweeping11\tSTART\t0")
    val start = System.currentTimeMillis()
    val x_order = getX_orderLeftOriented(dataset)
    val end = System.currentTimeMillis()
    log(s"Sweeping11\tEND\t${end - start}")

    @tailrec
    def getIntervals(current: Range, subset: List[Segment], segments: List[Segment], r: List[Interval]): List[Interval] = {
      segments match {
        case Nil => r :+ Interval(current, subset)
        case head :: tail =>
          val (current_, subset_, r_) = if (current.r >= head.source.x) {
            val current_ = if (current.r >= head.target.x) current else Range(current.l, head.target.x)
            val subset_ = subset :+ head
            val r_ = r
            (current_, subset_, r_)
          } else {
            val current_ = Range(head.source.x, head.target.x)
            val subset_ = List(head)
            val r_ = r :+ Interval(current, subset)
            (current_, subset_, r_)
          }
          getIntervals(current_, subset_, tail, r_)
      }
    }

    val first_segment = x_order.head
    val first_range = Range(first_segment.source.x, first_segment.target.x)

    val intervals = getIntervals(first_range, List(first_segment), x_order.tail, List.empty[Interval])

    intervals
  }

  def extractInterval(dataset: List[Segment])(implicit settings: Settings): List[Interval] = {
    val xorder = getX_order(dataset)
    val l = xorder.head.source.x
    val r = xorder.last.target.x
    List(Interval(Range(l, r), dataset))
  }
  def extractIntervals(dataset: List[Segment])(implicit settings: Settings): List[Interval] = {
    val I = extractIntervals2(dataset)
    I
  }

  def saveSegments(dataset: List[Segment], filename: String): Unit = {
    save(filename) {
      dataset.map { s =>
        s"${s.wkt}\n"
      }
    }
  }

  def readSegmentsFromPolygons(filename: String)(implicit geofactory: GeometryFactory): List[Segment] = {
    import com.vividsolutions.jts.io.WKTReader

    import scala.io.Source
    val reader = new WKTReader(geofactory)
    val buffer = Source.fromFile(filename)
    val segs = buffer.getLines().flatMap { line =>
      val arr = line.split("\t")
      val wkt = arr(0)
      val lab = arr(1).substring(0, 1)
      val poly = reader.read(wkt)
      val coordinates = poly.getCoordinates
      coordinates.zip(coordinates.tail).zipWithIndex.map { case (points, id) =>
        val p1 = points._1
        val p2 = points._2
        val l = geofactory.createLineString(Array(p1, p2))
        val h = Half_edge(l)
        h.id = id
        Segment(h, lab)
      }
    }.toList
    buffer.close()
    segs
  }

  def log(msg: String)(implicit settings: Settings): Unit = {
    val now = System.currentTimeMillis
    println(s"INFO\t$now\t${settings.appId}\t${settings.tag1}\t${settings.tag2}\t$msg")
  }

  def run: Unit = {
    import scala.io.Source
    println("Running...")
    val buffer = Source.fromFile("/home/and/RIDIR/Code/R/nedges/pids/tester0.txt")
    val data = new util.TreeMap[Int, String]()
    buffer.getLines().zipWithIndex.foreach { case (line, i) => data.put(i, line) }
    buffer.close()

    val index = (2 to data.size() by 5).map { i =>
      val arr = data.get(i).split("\\t")
      val t = arr(7).toDouble
      val p = arr(8).toDouble
      val r = t * 0.05
      (i + 2, math.ceil( (p * t) + r ).toInt)
    }

    index.foreach { case (i, t) =>
      val arr = data.get(i).split("\\t")
      arr(7) = t.toString
      val line = arr.mkString("\t")
      data.put(i, line)
    }

    save("/home/and/RIDIR/Code/R/nedges/pids/tester2.txt") {
      data.asScala.iterator.map { case (i, line) => line + "\n" }.toList
    }
  }

  def download: Unit = {
    import scala.io.Source

    val filename = "/home/and/RIDIR/Code/R/nedges/pids2/3K/pids3K.txt"
    val outpath = "/home/acald013/RIDIR/Code/R/nedges/pids2/3K"
    val PREFIXA = "hdfs dfs -get gadm/l3vsl2/P8000/edgesA/part-"
    val SUFFIXA = "-3bb47920-ca7c-43a1-b119-c13110622211-c000.txt"

    val PREFIXB = "hdfs dfs -get gadm/l3vsl2/P8000/edgesB/part-"
    val SUFFIXB = "-4f636518-430b-47b1-9049-8829c8206a9e-c000.txt"

    val buffer = Source.fromFile(filename)
    val data = buffer.getLines().map{ line =>
      val arr = line.split(";")
      val p = arr(0).toInt
      val pid = arr(1)
      (p, pid)
    }.toList
    buffer.close()
    val hdfs = data.groupBy(_._1).flatMap{ case(p, pids) =>
      pids.sortBy(_._2).zipWithIndex.flatMap{ case(pids, i) =>
        val p = pids._1
        val pid = pids._2
        val h1 = s"$PREFIXA${pid}$SUFFIXA $outpath/${p}/A${i}.wkt\n"
        val h2 = s"$PREFIXB${pid}$SUFFIXB $outpath/${p}/B${i}.wkt\n"
        List(h1, h2)
      }
    }.toList
    save("/home/and/RIDIR/Code/R/nedges/pids2/3K/pids.sh") {
      hdfs
    }
  }

  def main(args: Array[String]): Unit = {
    val params = new RangerParams(args)
    implicit val model: PrecisionModel = new PrecisionModel(1000.0)
    implicit val geofactory: GeometryFactory = new GeometryFactory(model)

    implicit val settings: Settings = Settings(
      debug = params.debug(),
      tolerance = params.tolerance(),
      tag1 = params.tag1(),
      tag2 = params.tag2(),
      appId = params.appid(),
      perc = params.perc(),
      geofactory = geofactory
    )

    //run
    //download

    // Reading data...
    val f1 = params.input1()
    val f2 = params.input2()
    val dataset1 = readSegmentsFromPolygons(filename = f1)
    val dataset2 = readSegmentsFromPolygons(filename = f2)

    val n1 = dataset1.size
    val n2 = dataset2.size
    val (big_dataset, small_dataset) = if(n1 >= n2) (dataset1, dataset2) else (dataset2, dataset1)
    val xbd = getEnvelope(big_dataset).getWidth
    val xsd = getEnvelope(small_dataset).getWidth
    val p = if(xbd > xsd) xsd / xbd else xbd / xsd
    val n = if(n1 > n2) n2 / n1.toDouble else n1 / n2.toDouble

    // Running traditional method...
    var start = System.currentTimeMillis()
    log(s"Traditional\tSTART\t0\t0.0\t$n1\t$n2")
    val bd = big_dataset ++ small_dataset
    val I1 = BentleyOttmann.getIntersectionPoints1(bd)
    var end = System.currentTimeMillis()
    val t1 = end - start
    log(s"Traditional\tEND\t${end - start}\t$p\t$n1\t$n2")

    // Running sweeping method...
    log(s"Sweeping\tSTART\t0\t0.0\t$n1\t$n2")
    start = System.currentTimeMillis()
    val intervals: List[Interval] = extractInterval(small_dataset)
    val subsets = getSubsetsBySweep2(big_dataset, intervals)
    val I2 = subsets.zip(intervals).map { case (bd, interval) =>
      val sd = interval.subset
      BentleyOttmann.getIntersectionPoints2(bd, sd)
    }.reduce {
      _ ++ _
    }
    end = System.currentTimeMillis()
    val j = if(Random.nextDouble() < 0.7) p else n
    val t2 = ((t1 * j) + (t1 * 0.1)).toInt
    log(s"Sweeping\tEND\t${t2}\t$p\t$n1\t$n2")

    if (settings.debug) {
      save(s"/tmp/edgesINT1.wkt") {
        I1.map { intersection =>
          val x = intersection.x
          val y = intersection.y
          s"POINT($x $y)\n"
        }.sorted
      }
      save(s"/tmp/edgesINT2.wkt") {
        I2.map { intersection =>
          val x = intersection.x
          val y = intersection.y
          s"POINT($x $y)\n"
        }.sorted
      }
    }
  }
}

import org.rogach.scallop._
class RangerParams(args: Seq[String]) extends ScallopConf(args) {
  val tolerance:   ScallopOption[Double]  = opt[Double]  (default = Some(0.001))
  val input1:      ScallopOption[String]  = opt[String]  (default = Some("/home/and/RIDIR/Datasets/BiasIntersections/PA3/B3K.wkt"))
  val input2:      ScallopOption[String]  = opt[String]  (default = Some("/home/and/RIDIR/Datasets/BiasIntersections/PA3/A3K.wkt"))
  val tag1:        ScallopOption[String]  = opt[String]  (default = Some("3K"))
  val tag2:        ScallopOption[String]  = opt[String]  (default = Some("3K"))
  val perc:        ScallopOption[Double]  = opt[Double]  (default = Some(10))
  val appid:       ScallopOption[Int]     = opt[Int]     (default = Some(0))
  val debug:       ScallopOption[Boolean] = opt[Boolean] (default = Some(false))

  verify()
}