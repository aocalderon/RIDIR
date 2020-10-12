import $ivy.`org.datasyslab:geospark:1.2.0`

import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel, Geometry, LineString, Coordinate}
import com.vividsolutions.jts.operation.linemerge.LineMerger

import collection.JavaConverters._
import scala.annotation.tailrec

val model = new PrecisionModel(1000)
val geofactory = new GeometryFactory(model)

val a = new Coordinate(0,0)
val b = new Coordinate(0,1)
val c = new Coordinate(1,2)
val d = new Coordinate(2,2)
val e = new Coordinate(3,1)
val f = new Coordinate(3,0)

val g = new Coordinate(4,0)
val h = new Coordinate(4,1)
val i = new Coordinate(4,2)
val j = new Coordinate(5,1)
val k = new Coordinate(5,0)
val l = new Coordinate(3,3)
val m = new Coordinate(4,3)
val n = new Coordinate(4,4)
val o = new Coordinate(3,3)

val s1 = geofactory.createLineString(Array(b,a,f,e))
val s2 = geofactory.createLineString(Array(e,d))
val s3 = geofactory.createLineString(Array(d,c))
val s4 = geofactory.createLineString(Array(c,b))

val s5 = geofactory.createLineString(Array(h,g,k,j))
val s6 = geofactory.createLineString(Array(j,i))
val s7 = geofactory.createLineString(Array(i,h))

val s8 = geofactory.createLineString(Array(l,m))
val s9 = geofactory.createLineString(Array(m,n))
val s0 = geofactory.createLineString(Array(n,o))

val s = List(s1,s2,s4,s8,s3,s5,s7,s9,s6,s0)

s.map{_.toText}.foreach(println)

val merger = new LineMerger()

s.foreach{merger.add}

println
merger.getMergedLineStrings().asScala.foreach{println}

/****************************************************************/
@tailrec
def findNext(s1: LineString, ss: List[LineString]): (LineString, List[LineString]) = {
  val last = s1.getCoordinates.last
  val (s, new_ss) = ss.partition{_.getCoordinates.head == last}
  if(s.isEmpty){
    (s1, ss)
  } else {
    val s2 = s.head
    val coords = s1.getCoordinates ++ s2.getCoordinates.tail
    val new_s1 = geofactory.createLineString(coords)

    findNext(new_s1, new_ss)
  }
}

@tailrec
def concatSegments(segments: List[LineString], r: List[LineString]): List[LineString] = {
  if(segments.isEmpty){
    r
  } else {
    val (r1, s1) = findNext(segments.head, segments.tail)
    concatSegments(s1, r :+ r1)
  }
}

println
val r = concatSegments(s, List.empty[LineString])
r.map{_.toText}.foreach{println}

val ss = s.flatMap{ seg =>
  val coords = seg.getCoordinates
  coords.zip(coords.tail).map{ case(c1, c2) =>
    geofactory.createLineString(Array(c1,c2))
  }
}.reverse

println
ss.foreach{println}

concatSegments(ss, List.empty[LineString]).foreach{println}
