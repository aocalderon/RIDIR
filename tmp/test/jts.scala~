import $ivy.`org.datasyslab:geospark:1.2.0`

import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory, Coordinate, LineString}
import com.vividsolutions.jts.io.WKTReader
import com.vividsolutions.jts.geomgraph.Edge
import scala.io.Source

case class HEdge(coords: Array[Coordinate], l: LineString) extends Edge(coords)

val scale1 = 1e14
val model = new PrecisionModel(scale1)
val geofactory = new GeometryFactory(model)
val reader = new WKTReader(geofactory)
println(s"SCALE 1: ${scale1}")

val a = reader.read("LINESTRING (-72.73115829200609 -36.4101238548755, -72.72614287999998 -36.40122986)").asInstanceOf[LineString] 
val b = reader.read("LINESTRING (-72.87486266999998 -36.41012385487555, -71.71875 -36.41012385487556)").asInstanceOf[LineString] 

val i = a.intersection(b)
//println(i.toText)

case class Data(arr: Array[String]){
  val pid = arr(0)
  val rid = arr(1).toInt
  val eid = arr(2).toInt

  override def toString: String = s"$pid\t$eid"
}

val bufferA = Source.fromFile("/home/and/RIDIR/tmp/test/edgesHedgesA.wkt")
val A = bufferA.getLines.toList
  .map{ a =>
    val arr = a.split("\t")
    val line = reader.read(arr(0)).asInstanceOf[LineString]
    val data = Data(arr.tail)
    line.setUserData(data)
    line
  }
bufferA.close

val bufferB = Source.fromFile("/home/and/RIDIR/tmp/test/edgesHedgesB.wkt")
val B = bufferB.getLines.toList
  .map{ a =>
    val arr = a.split("\t")
    val line = reader.read(arr(0)).asInstanceOf[LineString]
    val data = Data(arr.tail)
    line.setUserData(data)
    line
  }
bufferB.close

import scala.collection.JavaConverters._
val aList = A.filter{ a =>
  val data = a.getUserData.asInstanceOf[Data]
  
  data.pid == "A41045" && data.eid == 1
}.map{ h =>
  val pts = h.getCoordinates
  HEdge(pts, h)
}.asJava
println(aList.size())
aList.asScala map{a => a.l.toText + "\t" + a.l.getUserData} foreach println

val bList = B.filter{ b =>
  val data = b.getUserData.asInstanceOf[Data]
  
  data.pid == "B34716" && data.eid == 274
}.map{ h =>
  val pts = h.getCoordinates
  HEdge(pts, h)
}.asJava
println(bList.size())
bList.asScala map{b => b.l.toText + "\t" + b.l.getUserData} foreach println

import com.vividsolutions.jts.geomgraph.index.SimpleMCSweepLineIntersector
import com.vividsolutions.jts.geomgraph.index.SegmentIntersector
import com.vividsolutions.jts.algorithm.RobustLineIntersector
                          
val sweepline = new SimpleMCSweepLineIntersector()
val lineIntersector = new RobustLineIntersector()
//val scale2 = 1e0
//lineIntersector.setPrecisionModel(new PrecisionModel(scale2))
//println(s"SCALE 2: ${scale2}")
val segmentIntersector = new SegmentIntersector(lineIntersector, true, true)
sweepline.computeIntersections(aList, bList, segmentIntersector)

import com.vividsolutions.jts.geomgraph.EdgeIntersection
def getIntersections(list: java.util.List[HEdge]): Map[Coordinate, List[LineString]] = {
  list.asScala.flatMap{ edge =>
    edge.getEdgeIntersectionList.iterator.asScala.map{ i =>
      (i.asInstanceOf[EdgeIntersection].getCoordinate, edge.l)
    }.toList
  }.toList.groupBy(_._1).mapValues(_.map(_._2)).toMap
}

val intersA = getIntersections(aList)
println
println(s"# Intersection: ${intersA.size}")
intersA map(a => s"POINT(${a._1.x} ${a._1.y}") foreach println

val intersB = getIntersections(bList)
//intersB foreach println

save("/tmp/edgesA.wkt"){
  intersA.map{ case(coord, lines) =>
    val p = s"POINT(${coord.x} ${coord.y})"
    val l = lines.map(_.toText).mkString("\t")
    s"$p\t$l\n"
  }.toList
}
//println(intersA.size)
save("/tmp/edgesB.wkt"){
  intersB.map{ case(coord, lines) =>
    val p = s"POINT(${coord.x} ${coord.y})"
    val l = lines.map(_.toText).mkString("\t")
    s"$p\t$l\n"
  }.toList
}
//println(intersB.size)

def save(filename: String)(content: Seq[String]): Unit = {
  val start = clocktime
  val f = new java.io.PrintWriter(filename)
  f.write(content.mkString(""))
  f.close
  val end = clocktime
  val time = "%.2f".format((end - start) / 1000.0)
}
private def clocktime = System.currentTimeMillis()
