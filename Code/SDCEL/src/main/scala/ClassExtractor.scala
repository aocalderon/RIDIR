import org.apache.spark.{SparkConf, SparkContext}
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory, Coordinate, Point}
import org.apache.log4j.{Level, Logger}

import KMeansFinder.Data

object ClassExtractor {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    val geofactory = new GeometryFactory(new PrecisionModel(1e8))

    val conf = new SparkConf().setAppName("KMeansExample")
    val sc = new SparkContext(conf)

    val class_id = args(0).toInt
    val border_x = args(1).toDouble
    val border_y = args(2).toDouble
    val limit = args(3).toLong

    val border = geofactory.createPoint(new Coordinate(border_x, border_y))

    val reader = new com.vividsolutions.jts.io.WKTReader(geofactory)
    val buffer = scala.io.Source.fromFile(s"/tmp/edgesClass${class_id}.wkt")
    val cells = buffer.getLines.map{ line =>
      val arr = line.split("\t")
      val wkt = arr(0)
      val cid = arr(1).toInt
      val lid = arr(2).toInt
      val dis = arr(3).toDouble
      val ned = arr(4).toLong
      val cum = arr(5).toLong

      val point = reader.read(wkt)
      Data(wkt, cid, lid, dist = point.distance(border), ned, 0L)
    }.toList
    buffer.close

    val sorted = cells.sortBy(_.dist)
    val cumulative = sorted.scanLeft(0L)( (a, b) => a + b.nedges)
    println(s"# of edges per class $class_id: ${sorted.map(_.nedges).reduce(_ + _)}")
    val data = sorted.zip(cumulative).map{ case(data, cumulative) =>
      data.copy(cumulative = cumulative)
    }.filter(_.cumulative <= limit)
    val f = new java.io.PrintWriter(s"/tmp/edgesFClass${class_id}.wkt")
    f.write(data.mkString(""))
    f.close

  }


}
