import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory, Polygon}
import com.vividsolutions.jts.io.WKTReader

import scala.io.Source
import java.io.PrintWriter

object PolygonReader {
  def main(args:Array[String]) = {
    val scale = 1000.0
    implicit val model = new PrecisionModel(scale)
    implicit val geofactory = new GeometryFactory(model)
    implicit val reader = new WKTReader(geofactory)

    def read(filename: String): List[String] = {
      val buffer = Source.fromFile(filename)
      val polys = buffer.getLines.map{ wkt =>
        val poly = reader.read(wkt).asInstanceOf[Polygon]
        poly.toText()
      }.toList
      buffer.close
      polys
    }

    def write(filename: String, polys: List[String]): Unit = {
      val f1 = new PrintWriter(filename)
      f1.write(polys.map{wkt => s"$wkt\n"}.mkString(""))
      f1.close
    }

    val inputA = "/home/acald013/Datasets/TX/TX_2000_2272.wkt"
    val inputB = "/home/acald013/Datasets/TX/TX_2010_2272.wkt"
    
    val polysA = read(inputA)
    val polysB = read(inputB)

    val outputA = "/home/acald013/Datasets/TX/TXA_3dec.wkt"
    val outputB = "/home/acald013/Datasets/TX/TXB_3dec.wkt"

    write(outputA, polysA)
    println(s"Write in $outputA")
    write(outputB, polysB)
    println(s"Write in $outputB")

  }
}
