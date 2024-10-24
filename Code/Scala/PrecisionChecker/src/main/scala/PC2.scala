import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory, Geometry, LineString}
import com.vividsolutions.jts.io.WKTReader

object PC2 {
  def test(awkt: String, bwkt: String, scale: Double): Geometry = {
    val model = new PrecisionModel(scale)
    val geofactory = new GeometryFactory(model)
    val reader = new WKTReader(geofactory)

    val a = reader.read(awkt).asInstanceOf[LineString]
    println(s"A: ${a.toText}")
    val b = reader.read(bwkt).asInstanceOf[LineString]
    println(s"B: ${b.toText}")

    a.intersection(b)
  }

  def main(args: Array[String]): Unit = {
    println("vividsolutions")
    val awkt = "LINESTRING (-72.73115829200609 -36.4101238548755, " +
      "-72.72614287999998 -36.40122986)"
    val bwkt = "LINESTRING (-72.87486266999998 -36.41012385487555, " +
      "-71.71875 -36.41012385487556)"
    (0 to 14).foreach{ i =>
      val scale = math.pow(10, i.toDouble)
      println(s"Scale: $scale")
      val r = test(awkt, bwkt, scale)
      println(s"Intersection results: ${r.toText()}")
    }


  }
}
