import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory, Polygon}
import com.vividsolutions.jts.geom.{Coordinate, Location}
import com.vividsolutions.jts.algorithm.PointLocator
import com.vividsolutions.jts.io.WKTReader

import scala.io.Source
import java.io.PrintWriter

object PointLocator2 {
  def main(args:Array[String]) = {
    val scale = 100000.0
    implicit val model = new PrecisionModel(scale)
    implicit val geofactory = new GeometryFactory(model)
    implicit val reader = new WKTReader(geofactory)

    def read(filename: String): List[Polygon] = {
      val buffer = Source.fromFile(filename)
      val polys = buffer.getLines.map{ wkt =>
        val poly = reader.read(wkt).asInstanceOf[Polygon]
        poly
      }.toList
      buffer.close
      polys
    }

    def write(filename: String, polys: List[String]): Unit = {
      val f1 = new PrintWriter(filename)
      f1.write(polys.map{wkt => s"$wkt\n"}.mkString(""))
      f1.close
    }

    val inputA = "/home/and/RIDIR/Datasets/Phili/philiA.wkt"
    val inputB = "/home/and/RIDIR/Datasets/Phili/philiB.wkt"
    
    val polysA = read(inputA)
    val polysB = read(inputB)

    val x = 2678580.509
    val y = 219615.861
    val coord = new Coordinate(x,y)
    val locator = new PointLocator

    val test = polysB.map{ poly =>
      val l = locator.locate(coord, poly)
      (Location.toLocationSymbol(l), poly)
    }.filter(_._1 == 'i').map(_._2)

    test.foreach{println}

  }
}
