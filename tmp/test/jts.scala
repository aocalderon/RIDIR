import $ivy.`org.datasyslab:geospark:1.2.0`

import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory, Coordinate, LineString}
import com.vividsolutions.jts.io.WKTReader
import scala.collection.JavaConverters._
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS

val scale1 = 1e14
val scale2 = 1e8
val wkt = "LINESTRING (-72.73115829200609 -36.4101238548755, -72.72614287999998 -36.40122986)"
//val wkt = "LINESTRING (-72.87486266999998 -36.41012385487555, -71.71875 -36.41012385487556)" 

val sourceCRS = CRS.decode("EPSG:4326")
val targetCRS = CRS.decode("EPSG:3857")
val transform = CRS.findMathTransform(sourceCRS, targetCRS, false);

val model1 = new PrecisionModel(scale1)
val geofactory1 = new GeometryFactory(model1)
val reader1 = new WKTReader(geofactory1)
val model2 = new PrecisionModel(scale2)
val geofactory2 = new GeometryFactory(model2)
val reader2 = new WKTReader(geofactory2)

println(s"SCALE 1: ${scale1}")
val a1 = reader1.read(wkt).asInstanceOf[LineString] 
val target1 = JTS.transform(a1, transform)
println(target1.toText)

println(s"SCALE 2: ${scale2}")
val a2 = reader2.read(wkt).asInstanceOf[LineString] 
val target2 = JTS.transform(a2, transform)
println(target2.toText)

def save(filename: String)(content: Seq[String]): Unit = {
  val start = clocktime
  val f = new java.io.PrintWriter(filename)
  f.write(content.mkString(""))
  f.close
  val end = clocktime
  val time = "%.2f".format((end - start) / 1000.0)
}
private def clocktime = System.currentTimeMillis()
