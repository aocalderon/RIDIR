import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory, Polygon}
import com.vividsolutions.jts.io.WKTReader

import scala.io.Source
import java.io.PrintWriter

object PolygonReader {
  def main(args:Array[String]) = {
    val scale = 100000.0
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

    val inputA = "/home/and/RIDIR/tmp/debug/comparePolys.wkt"
    val inputB = "/home/and/RIDIR/Datasets/Phili/philiB.wkt"
    
    val polysA = read(inputA)
    val polysB = read(inputB)

    val A = polysA.map{ wkt =>
      val p = reader.read(wkt).asInstanceOf[Polygon]
      p
    }
    val a = A(0)
    val b = A(1)

    println("a")
    println(a.toText)
    a.normalize
    println("a norm")
    println(a.toText)

    println("b")
    println(b.toText)
    b.normalize
    println("b norm")
    println(b.toText)

    val bool = a.equalsExact(b, 1)

    println(bool)

    //val outputA = "/home/and/RIDIR/Code/CGAL/DCEL/data/PH_A.wkt"
    //val outputB = "/home/and/RIDIR/Code/CGAL/DCEL/data/PH_B.wkt"

    //write(outputA, polysA)
    //write(outputB, polysB)

  }
}
