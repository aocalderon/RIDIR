package edu.ucr.dblab.sdcel.extension

import com.vividsolutions.jts.geom._
import com.vividsolutions.jts.io.WKTReader
import edu.ucr.dblab.sdcel.Params
import edu.ucr.dblab.sdcel.Utils.{Settings, log, save}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.slf4j.{Logger, LoggerFactory}

object ScaleUpBuilder {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]): Unit = {
    // Starting session...
    implicit val params: Params = new Params(args)
    implicit val spark: SparkSession = SparkSession.builder()
      .master(params.master())
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()
    implicit val S: Settings = Settings(
      tolerance = params.tolerance(),
      debug = params.debug(),
      appId = spark.sparkContext.applicationId
    )
    val tag = params.tag()
    val command = System.getProperty("sun.java.command")
    log(s"COMMAND|$command")

    log("TIME|Start")

    log(s"INFO|$tag|scale|${S.scale}")
    implicit val model: PrecisionModel = new PrecisionModel(S.scale)
    implicit val G: GeometryFactory = new GeometryFactory(model)
    implicit val reader: WKTReader = new WKTReader(G)

    // Reading data...
    val polygons = spark.read.textFile(params.input1()).rdd.mapPartitions{ lines =>
      val reader = new WKTReader(G)
      lines.map { line =>
        val arr = line.split("\t")
        val geom = reader.read(arr(0))
        val pid  = arr(1).toLong
        val center = geom.getCentroid
        val nedges = geom.getCoordinates.length

        (pid, center, nedges, geom)
      }
    }

    val boundary = SU_Utils.getEnvelope(polygons.map{_._4})
    boundary.expandBy(S.tolerance)
    val totalEdges = polygons.map(_._3).sum()
    val quadrant_size = totalEdges / 4

    def acc_geoms(polygons: RDD[(Long, Point, Int, Geometry)], corner: Point): List[Long] = {
      val sorted_polygons = polygons.map{ case(pid, center, nedges, _) =>
        val d = center.distance(corner)
        (d, (pid, nedges))
      }.sortBy(_._1).map(_._2).collect()

      var acc = 0
      val corner_acc = for(i <- sorted_polygons.indices) yield {
        val (pid, n) = sorted_polygons(i)
        acc += n
        (pid, acc)
      }
      corner_acc.filter(_._2 < quadrant_size).map(_._1).toList
    }

    def acc_polygons(polygons: RDD[(Long, Point, Int, Polygon)], corner: Point): List[Long] = {
      val sorted_polygons = polygons.map{ case(pid, center, nedges, _) =>
        val d = center.distance(corner)
        (d, (pid, nedges))
      }.sortBy(_._1).map(_._2).collect()

      var acc = 0
      val corner_acc = for(i <- sorted_polygons.indices) yield {
        val (pid, n) = sorted_polygons(i)
        acc += n
        (pid, acc)
      }
      corner_acc.filter(_._2 < quadrant_size).map(_._1).toList
    }

    val label = "B"
    val path_ca = "/home/acald013/Datasets/MainUS/S"

    val NW = G.createPoint(new Coordinate(boundary.getMinX, boundary.getMaxY))
    val p1 = acc_geoms(polygons, NW)
    save(s"$path_ca/${label}1.wkt"){
      polygons.filter{ case(pid, _, _, _) =>
        p1.contains(pid)
      }.map{ case(pid, center, n, p) =>
        val wkt = p.toText
        s"$wkt\t$pid\n"
      }.collect()
    }
    polygons.filter(p => p1.contains(p._1)).map(_._3).sum()

    val NE = G.createPoint(new Coordinate(boundary.getMaxX, boundary.getMaxY))
    val polygons2 = polygons.filter{ case(pid, _, _, _) => !p1.contains(pid) }
    val p2 = acc_geoms(polygons2, NE)
    save(s"$path_ca/${label}2.wkt"){
      polygons.filter{ case(pid, _, _, _) =>
        p2.contains(pid)
      }.map{ case(pid, center, n, p) =>
        val wkt = p.toText
        s"$wkt\t$pid\n"
      }.collect()
    }

    val SW = G.createPoint(new Coordinate(boundary.getMinX, boundary.getMinY))
    val polygons3 = polygons2.filter{ case(pid, _, _, _) => !p2.contains(pid) }
    val p3 = acc_geoms(polygons3, SW)
    save(s"$path_ca/${label}3.wkt"){
      polygons.filter{ case(pid, _, _, _) =>
        p3.contains(pid)
      }.map{ case(pid, center, n, p) =>
        val wkt = p.toText
        s"$wkt\t$pid\n"
      }.collect()
    }

    val SE = G.createPoint(new Coordinate(boundary.getMinX, boundary.getMinY))
    val polygons4 = polygons3.filter{ case(pid, _, _, _) => !p3.contains(pid) }
    val p4 = acc_geoms(polygons4, SE)
    save(s"$path_ca/${label}4.wkt"){
      polygons.filter{ case(pid, _, _, _) =>
        p4.contains(pid)
      }.map{ case(pid, center, n, p) =>
        val wkt = p.toText
        s"$wkt\t$pid\n"
      }.collect()
    }

    val n1 = polygons.filter(p => p1.contains(p._1)).map(_._3).sum()
    println(n1)
    val n2 = polygons.filter(p => p2.contains(p._1)).map(_._3).sum()
    println(n2)
    val n3 = polygons.filter(p => p3.contains(p._1)).map(_._3).sum()
    println(n3)
    val n4 = polygons.filter(p => p4.contains(p._1)).map(_._3).sum()
    println(n4)

    spark.close
  }
}
