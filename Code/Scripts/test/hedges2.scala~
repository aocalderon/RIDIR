import $ivy.`org.datasyslab:geospark:1.2.0`
import com.vividsolutions.jts.geom.{Coordinate, LineString, Polygon, Point}
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import scala.annotation.tailrec

val model = new PrecisionModel(1000)
implicit val geofactory = new GeometryFactory(model)

val l1 = geofactory.createLineString(Array(new Coordinate(1,1), new Coordinate(5,5)))
l1.setUserData(EdgeData(1,0,1,false))
val l5 = geofactory.createLineString(Array(new Coordinate(5,5), new Coordinate(9,1)))
l5.setUserData(EdgeData(1,0,2,false))

val l2 = geofactory.createLineString(Array(new Coordinate(9,1), new Coordinate(5,5)))
l2.setUserData(EdgeData(2,0,1,false))
val l6 = geofactory.createLineString(Array(new Coordinate(5,5), new Coordinate(9,9)))
l6.setUserData(EdgeData(2,0,2,false))

val l3 = geofactory.createLineString(Array(new Coordinate(9,9), new Coordinate(5,5)))
l3.setUserData(EdgeData(3,0,1,false))
val l7 = geofactory.createLineString(Array(new Coordinate(5,5), new Coordinate(1,9)))
l7.setUserData(EdgeData(3,0,2,false))

val l4 = geofactory.createLineString(Array(new Coordinate(1,9), new Coordinate(5,5)))
l4.setUserData(EdgeData(4,0,1,false))
val l8 = geofactory.createLineString(Array(new Coordinate(5,5), new Coordinate(1,1)))
l8.setUserData(EdgeData(4,0,2,false))

val h1 = Half_edge(l1)
val h2 = Half_edge(l2)
val h3 = Half_edge(l3)
val h4 = Half_edge(l4)
val h5 = Half_edge(l5)
val h6 = Half_edge(l6)
val h7 = Half_edge(l7)
val h8 = Half_edge(l8)

val hedges = List(h1,h2,h3,h4,h5,h6,h7,h8)

val f = new java.io.PrintWriter("/tmp/edgesT.wkt")
f.write(hedges.map{ h => s"${h.edge.toText}\t${h.data}\n"}.mkString(""))
f.close

case class H(vertex: Vertex, hedge: Half_edge, angle: Double)

hedges.flatMap{ h =>
  List(
    H(h.orig, h, h.angleAtOrig),
    H(h.dest, h, h.angleAtDest)
  )
}.groupBy(h => (h.vertex, h.angle)).values.foreach{ hList =>
  val h0 = hList(0).hedge
  val h1 = hList(1).hedge

  h0.twin = h1
  h1.twin = h2
}

hedges.filter(_.data.edgeId == 2).foreach{_.printTwin}

case class EdgeData(polygonId: Int, ringId: Int, edgeId: Int, isHole: Boolean,
  label: String = "A") {
  override def toString: String = s"${label}$polygonId\t$ringId\t$edgeId\t$isHole"
}

case class Half_edge(edge: LineString) {
  private val coords = edge.getCoordinates
  val v1 = coords(0)
  val v2 = coords(1)
  val orig = Vertex(v1)
  val dest = Vertex(v2)
  val data = edge.getUserData.asInstanceOf[EdgeData]
  val wkt = edge.toText()
  var twin: Half_edge = null
  var next: Half_edge = null
  var prev: Half_edge = null

  def getPolygon(implicit geofactory: GeometryFactory): Polygon = {
    val coords = v1 +: getNexts.map{_.v2}
    geofactory.createPolygon(coords.toArray)
  }

  def getNextsAsWKT(implicit geofactory: GeometryFactory): String = {
    val hedges = getNexts
    val coords = hedges.map{_.v1} :+ hedges.last.v2
    geofactory.createLineString(coords.toArray).toText
  }

  private def getNexts: List[Half_edge] = {
    @tailrec
    def getNextsTailrec(hedges: List[Half_edge]): List[Half_edge] = {
      val next = hedges.last.next
      if( next == null || next == hedges.head){
        hedges
      } else {
        getNextsTailrec(hedges :+ next)
      }
    }
    getNextsTailrec(List(this))
  }

  val angleAtOrig = math.toDegrees(hangle(v1.x - v2.x, v1.y - v2.y))
  val angleAtDest = math.toDegrees(hangle(v2.x - v1.x, v2.y - v1.y))

  private def hangle(dx: Double, dy: Double): Double = {
    val length = math.sqrt( (dx * dx) + (dy * dy) )
    if(dy > 0){
      math.acos(dx / length)
    } else {
      2 * math.Pi - math.acos(dx / length)
    }
  }

  def reverse(implicit geofactory: GeometryFactory): Half_edge = {
    val edge = geofactory.createLineString(Array(v2, v1))
    edge.setUserData(EdgeData(-1,0,0,false))
    Half_edge(edge)
  }

  def printTwin = println(s"${this}'twin is ${this.twin}")
}

case class Vertex(coord: Coordinate, hedges: List[Half_edge] = List.empty[Half_edge]) {
  val x = coord.x
  val y = coord.y

  def hedgesNum: Int = hedges.size

  def getIncidentHedges: List[Half_edge] = {
    this.hedges.filter(h => h.dest.x == x && h.dest.y == y)
      .sortBy(_.angleAtDest)(Ordering[Double].reverse)
  }

  def toPoint(implicit geofactory: GeometryFactory): Point = geofactory.createPoint(coord)
}
