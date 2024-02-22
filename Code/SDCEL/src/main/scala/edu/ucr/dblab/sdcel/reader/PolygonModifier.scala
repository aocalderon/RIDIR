package edu.ucr.dblab.sdcel.reader

import com.vividsolutions.jts.geom.{GeometryFactory, Polygon, PrecisionModel}
import com.vividsolutions.jts.io.WKTReader

import scala.io.Source
import edu.ucr.dblab.sdcel.Utils.save

object PolygonModifier {
  def main(args: Array[String]): Unit = {
    val tolerance = 1e-3
    implicit val M: PrecisionModel = new PrecisionModel(1.0 / tolerance)
    implicit val G: GeometryFactory = new GeometryFactory(M)

    val reader = new WKTReader(G)
    val buffer = Source.fromFile("/home/acald013/RIDIR/Datasets/Phili/B.wkt")
    val polygons = buffer.getLines().map { line =>
      val arr = line.split("\t")
      val wkt = arr(0)
      val polygon = reader.read(wkt).asInstanceOf[Polygon]
      val id = arr(1).toInt
      polygon.setUserData(id)
      polygon
    }.toList
    buffer.close()

    val (sample1, sample2) = polygons.partition(_.getUserData.asInstanceOf[Int] < 100)

    val segments = sample1.flatMap { polygon =>
      val pid = polygon.getUserData.asInstanceOf[Int]
      val coords = polygon.getCoordinates

      coords.zip(coords.tail).zipWithIndex.map { case (cs, sid) =>
        val segment = G.createLineString(Array(cs._1, cs._2))
        val wkt = segment.toText
        s"$wkt\t$pid\t$sid\n"
      }
    }
    save("/tmp/edgesSegments.wkt") {
      segments
    }

    val open_segments = sample1.flatMap { polygon =>
      val pid = polygon.getUserData.asInstanceOf[Int]
      val coords = polygon.getCoordinates
      val open_coords = coords.slice(0, coords.size - 2)

      open_coords.zip(open_coords.tail).zipWithIndex.map { case (cs, sid) =>
        val segment = G.createLineString(Array(cs._1, cs._2))
        val wkt = segment.toText
        s"$wkt\t$pid\t$sid\n"
      }
    }
    save("/tmp/edgesOpenSegments.wkt") {
      open_segments
    }

    val open_polygons = sample1.map { polygon =>
      val pid = polygon.getUserData.asInstanceOf[Int]
      val coords = polygon.getCoordinates
      val open_coords = coords.slice(0, coords.size - 2)

      val open_polygon = G.createLineString(open_coords)
      open_polygon.setUserData(pid)
      open_polygon
    }
    save("/tmp/edgesOpenPolygons.wkt") {
      open_polygons.map { open =>
        val wkt = open.toText
        val pid = open.getUserData.asInstanceOf[Int]

        s"$wkt\t$pid\n"
      }
    }

    val output = open_polygons ++ sample2

    save("/tmp/edgesGeometries.wkt"){
      output.map{ geom =>
        val wkt = geom.toText
        val gid = geom.getUserData.asInstanceOf[Int]

        s"$wkt\t$gid\n"
      }
    }

  }
}
