import $ivy.`org.datasyslab:geospark:1.2.0`

import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory, Coordinate, Polygon}
import scala.annotation.tailrec

val model = new PrecisionModel(1000)
val geofactory = new GeometryFactory(model)

case class P(id: Int, geom: Polygon, var holes: Vector[P] = Vector.empty[P])

val c1 = Array(new Coordinate(0,0), new Coordinate(3,0), new Coordinate(3,3), new Coordinate(0,3), new Coordinate(0,0))
val c2 = Array(new Coordinate(1,1), new Coordinate(2,1), new Coordinate(2,2), new Coordinate(1,2), new Coordinate(1,1))
val c3 = Array(new Coordinate(4,1), new Coordinate(6, 1), new Coordinate(6,3), new Coordinate(4,1))
val c4 = Array(new Coordinate(0.25,0.25), new Coordinate(0.5, 0.25), new Coordinate(0.5,0.5), new Coordinate(0.25,0.25))

val p1 = P(1, geofactory.createPolygon(c1))
val p2 = P(2, geofactory.createPolygon(c2))
val p3 = P(3, geofactory.createPolygon(c3))
val p4 = P(4, geofactory.createPolygon(c4))

val exteriors = Vector(p1, p3)
val holes = Vector(p2, p4)

@tailrec
def matchHoles(holes: List[P], exteriors: Vector[P]): Vector[P] = {
  holes match {
    case Nil => exteriors
    case head +: tail => {
      val exterior = exteriors.find{ exterior => head.geom.coveredBy(exterior.geom) }.get
      exterior.holes = exterior.holes :+ head

      matchHoles(tail, exteriors)
    }
  }
}

matchHoles(holes.toList, exteriors).foreach{println}


