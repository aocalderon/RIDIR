import scala.util.{Try,Success,Failure}
import scala.collection.JavaConverters._
import scala.collection.mutable.{ListBuffer, HashSet, ArrayBuffer}
import scala.annotation.tailrec
import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel, Geometry}
import com.vividsolutions.jts.geom.{Coordinate, LinearRing, MultiPolygon, Polygon, LineString, Point}

class GraphEdge(pts: Array[Coordinate], hedge: Half_edge) extends com.vividsolutions.jts.geomgraph.Edge(pts) {
  private val geofactory: GeometryFactory = new GeometryFactory(new PrecisionModel(1000));

  def getVerticesSet: List[Vertex] = {
    var vertices = new ArrayBuffer[Vertex]()
    vertices += hedge.v1
    super.getEdgeIntersectionList().iterator().asScala.toList.foreach{ n =>
      val inter = n.asInstanceOf[com.vividsolutions.jts.geomgraph.EdgeIntersection]
      val coord = inter.getCoordinate
      vertices += Vertex(coord.x, coord.y)
    }
    vertices += hedge.v2
    vertices.toList.distinct
  }

  def getGraphEdges: List[GraphEdge] = {
    val vertices = this.getVerticesSet.map(v => new Coordinate(v.x, v.y))
    val segments = vertices.zip(vertices.tail)
    for{
      segment <- segments
    } yield {
      val pts = Array(segment._1, segment._2)
      new GraphEdge(pts, hedge)
    }
  }

  def getLineStrings: List[LineString] = {
    val vertices = this.getVerticesSet.map(v => new Coordinate(v.x, v.y))
    val segments = vertices.zip(vertices.tail)
    for(segment <- segments) yield {
      val arr = Array(segment._1, segment._2)
      val line = geofactory.createLineString(arr)
      val tag = this.hedge.tag
      val label = this.hedge.label
      val id = this.hedge.id
      val ring = this.hedge.ring
      val order = this.hedge.order
      line.setUserData(s"$id\t$ring\t$order\t$tag$label")
      line
    }
  }

  def getHalf_edges: List[Half_edge] = {
    var half_edges = new ArrayBuffer[Half_edge]()
    val vertices = this.getVerticesSet
    val segments = vertices.zip(vertices.tail)
    for(segment <- segments){
      val he = Half_edge(segment._1, segment._2)
      he.tag = this.hedge.tag
      he.label = this.hedge.label
      he.id = this.hedge.id
      he.ring = this.hedge.ring
      he.order = this.hedge.order
      half_edges += he
    }
    half_edges.toList.distinct
  }

  def getVerticesAndIncidents: List[(Half_edge, Vertex)] = {
    this.getHalf_edges.map(hedge => (hedge, hedge.v1)).toList
  }

  def getIntersectionPoints: List[(Point, Half_edge)] = {
    val intersections = super.getEdgeIntersectionList()
    intersections.iterator().asScala.toList.map{ n =>
      val inter = n.asInstanceOf[com.vividsolutions.jts.geomgraph.EdgeIntersection]
      val coord = inter.getCoordinate
      val seg = inter.getSegmentIndex
      val dist = inter.getDistance
      val point = geofactory.createPoint(coord)
      point.setUserData(s"$seg\t$dist")
      (point, hedge)
    }

  }

  def toWKT: String = {
    val intersections = super.getEdgeIntersectionList()
    intersections.iterator().asScala.toList.map{ n =>
      val inter = n.asInstanceOf[com.vividsolutions.jts.geomgraph.EdgeIntersection]
      val coord = inter.getCoordinate
      val seg = inter.getSegmentIndex
      val dist = inter.getDistance
      s"POINT(${coord.x} ${coord.y})\t$seg\t$dist\t${hedge.id}"
    }.mkString("\n")
  }
}

case class LDCEL(id: Int, vertices: Vector[Vertex], half_edges: Vector[Half_edge], faces: Vector[Face], tag: String = "A"){

  val nVertices = vertices.size
  val nHalf_edges = half_edges.size
  val nFaces = faces.size

  def union(): Vector[Face] = {
    faces.filter(_.area() > 0)
  }

  def intersection(): Vector[Face] = {
    faces.filter(_.id.split("\\|").size == 2)
  }

  def symmetricDifference(): Vector[Face] = {
    faces.filter(_.id.split("\\|").size == 1)
      .filter(_.area() > 0)
  }

  def differenceA(): Vector[Face] = {
    symmetricDifference().filter(_.id.size > 1).filter(_.id.substring(0, 1) == "A")
  }

  def differenceB(): Vector[Face] = {
    symmetricDifference().filter(_.id.size > 1).filter(_.id.substring(0, 1) == "B")
  }
}

case class LocalDCEL(half_edges: List[Half_edge], faces: List[Face], vertices: List[Vertex], edges: List[Edge] = null) {
  var id: Long = -1L
  var tag: String = ""
  var nEdges: Int = 0
  var executionTime: Long = 0L
}

case class MergedDCEL(half_edges: List[Half_edge], faces: List[Face], vertices: List[Vertex]) {
  
  def union(): List[Face] = {
    faces.filter(_.area() > 0)
  }

  def intersection(): List[Face] = {
    faces.filter(_.tag.split("|").size == 2)
  }

  def symmetricDifference(): List[Face] = {
    faces.filter(_.tag.split("|").size == 1)
      .filter(_.area() > 0)
  }

  def differenceA(): List[Face] = {
    symmetricDifference().filter(_.tag.size > 1).filter(_.tag.substring(0, 1) == "A")
  }

  def differenceB(): List[Face] = {
    symmetricDifference().filter(_.tag.size > 1).filter(_.tag.substring(0, 1) == "B")
  }
}

case class Half_edge(v1: Vertex, v2: Vertex) extends Ordered[Half_edge] {
  var id:    String = ""
  var ring:  Int    = -1
  var order: Int    = -1
  var origen: Vertex = v2
  var twin: Half_edge = null
  var next: Half_edge = null
  var prev: Half_edge = null
  var face: Face = null
  var label: String = null
  var tag: String = ""
  var visited: Boolean = false
  var isTwin: Boolean = false

  val angle  = math.toDegrees(hangle(v2.x - v1.x, v2.y - v1.y))
  val length = math.sqrt(math.pow(v2.x - v1.x, 2) + math.pow(v2.y - v1.y, 2))

  def hangle(dx: Double, dy: Double): Double = {
    val length = math.sqrt( (dx * dx) + (dy * dy) )
    if(dy > 0){
      math.acos(dx / length)
    } else {
      2 * math.Pi - math.acos(dx / length)
    }
  }

  override def compare(that: Half_edge): Int = {
    if (origen.x == that.origen.x) origen.y compare that.origen.y
    else origen.x compare that.origen.x
  }

  def canEqual(a: Any) = a.isInstanceOf[Half_edge]

  override def equals(that: Any): Boolean =
    that match {
      case that: Half_edge => {
        that.canEqual(this) && this.v1.equals(that.v1) && this.v2.equals(that.v2)
      }
      case _ => false
    }

  def equalsWithLabel(that: Half_edge): Boolean = {
    this.equals(that) && this.label == that.label
  }

  def toWKT: String = s"LINESTRING (${twin.origen.x} ${twin.origen.y} , ${origen.x} ${origen.y})\t$id"

  def toWKT2: String = s"LINESTRING (${v2.x} ${v2.y} , ${v1.x} ${v1.y})\t$id"

  def toWKT3: String = s"LINESTRING (${v1.x} ${v1.y} , ${v2.x} ${v2.y})\t$id"
}

case class Vertex(x: Double, y: Double) extends Ordered[Vertex] {
  private var _id: Double = -1
  var edge: Half_edge = null
  var half_edges: ArrayBuffer[Half_edge] = new ArrayBuffer[Half_edge]()
  var hedges_size: Int = 0

  def getId = _id

  def setId(id: Long): Unit = {
    _id = id
  }

  def setHalf_edges(hedges: List[Half_edge]): Unit = {
    this.half_edges.clear()
    this.half_edges ++= hedges.groupBy(_.angle)
      .flatMap{ hedge =>
        if(hedge._2.size > 1){ // Handling duplicate half edges and setting new twins...
          val h1s = hedge._2.filter(_.id != "*")
          val h2s = hedge._2.filter(_.id == "*")
          val h = (h1s.headOption, h2s.headOption) match {
            case (Some(h1), Some(h2)) => { // there are exterior and interior edges...
              h1.twin = h2.twin
              List(h1)
            }
            case (Some(h1), None) => { // there are only interior edges...
              h1s
            }
            case (None, Some(h2)) => { // there are only exterior edges..
              h2s
            }
            case (None, None) => List.empty[Half_edge] // Can't happen!
          }
          h
        } else {
          hedge._2
        }
      }
  }

  def getHalf_edges(): List[Half_edge] = {
    this.half_edges.toList.sortBy(_.angle)(Ordering[Double].reverse)
  }

  override def compare(that: Vertex): Int = {
    if (x == that.x) y compare that.y
    else x compare that.x
  }

  def canEqual(a: Any) = a.isInstanceOf[Vertex]

  override def equals(that: Any): Boolean =
    that match {
      case that: Vertex => {
        that.canEqual(this) && this.x == that.x && this.y == that.y
      }
      case _ => false
    }

  override def toString = s"$x $y"

  def toWKT: String = s"POINT($x $y)"
}

case class Face(label: String, cell: Int = -1) extends Ordered[Face]{
  private val geofactory: GeometryFactory = new GeometryFactory(new PrecisionModel(1000));
  var id: String = ""
  var ring: Int = -1
  var outerComponent: Half_edge = null
  var exterior: Boolean = false
  var tag: String = ""
  var innerComponents: Vector[Face] = Vector.empty[Face]

  @tailrec
  private def getNodes(start: Half_edge, end: Half_edge, v: Vector[Half_edge]): Vector[Half_edge] = {
    if(start == end){
      v :+ end
    } else {
      getNodes(start.next, end, v :+ start)
    }
  }

  def getHedges: Vector[Half_edge] = {
    val start = this.outerComponent
    getNodes(start, start.prev, Vector.empty[Half_edge])
  }

  def isSurroundedBy: Option[String] = {
    if(id == ""){
      None
    } else {
      val tag = id.substring(0, 1)
      val ids = getHedges.flatMap(_.twin.id.split("\\|"))
        .map{ id => if(id == "") "*" else id }
        .filter(_.substring(0, 1) != tag)

      if(ids.distinct.size == 1){
        Some(ids.distinct.head)
      } else {
        None
      }
    }
  }

  def nHalf_edges: Int = {
    getHedges.size
  }

  def faceArea(): Double = {
    var a: Double = 0.0
    var h = outerComponent
    while(h.next != outerComponent){
      val p1 = h.v2
      val p2 = h.next.v2
      a += (p1.x * p2.y) - (p2.x * p1.y)
      h = h.next
    }
    val p1 = h.v2
    val p2 = outerComponent.v2

    (a + (p1.x * p2.y) - (p2.x * p1.y)) / 2.0
  }

  /***
   * Compute area of irregular polygon
   * More info at https://www.mathopenref.com/coordpolygonarea2.html
   ***/
  def outerComponentArea(): Double = {
    var area: Double = 0.0
    var h = outerComponent
    do{
      area += (h.v1.x + h.v2.x) * (h.v1.y - h.v2.y)
      h = h.next
    }while(h != outerComponent)

    area / -2.0
  }

  def area(): Double = {
    val outer = faceArea()
    val inner = innerComponents.map(_.faceArea()).sum
    outer + inner
  }

  def perimeter(): Double = {
    var p: Double = 0.0
    var h = outerComponent
    while(h.next != outerComponent){
      p += h.length
      h = h.next
    }
    p += h.length
    p
  }

  def getLeftmostVertex(): Vertex = {
    var vertex = Vertex(Double.MaxValue, Double.MaxValue)
    var h = outerComponent
    do{
      if(h.v1.x < vertex.x){
        vertex = h.v1
      }
      if(h.v2.x < vertex.x){
        vertex = h.v2
      }
      h = h.next
    }while(h.next != outerComponent)
    vertex
  }

  def lowerleft: Vertex = {
    var h = outerComponent
    var vertex = h.v1
    do{
      if(h.v2 < vertex){
        vertex = h.v2
      }
      h = h.next
    }while(h.next != outerComponent)
    vertex
  }

  def getLinearRing(toWKT: Boolean = false): String = {
    var vertices = new ArrayBuffer[String]()
    var h = outerComponent
    vertices += s"${h.v1.x} ${h.v1.y}"
    do{
      vertices += s"${h.v2.x} ${h.v2.y}"
      h = h.next
    }while(h != outerComponentArea())
    if(toWKT){
      s"LINESTRING (${vertices.mkString(",")})"
    } else {
      s"(${vertices.mkString(",")})"
    }
  }

  private def toLine(reverse: Boolean = false): String = {
    var hedge = outerComponent
    var wkt = new ArrayBuffer[String]()
    wkt += s"${hedge.v1.x} ${hedge.v1.y}"
    while(hedge.next != outerComponent){
      wkt += s"${hedge.v2.x} ${hedge.v2.y}"
      hedge = hedge.next
    }
    wkt += s"${hedge.v2.x} ${hedge.v2.y}"
    if(reverse){
      s"(${wkt.reverse.mkString(",")})"
    } else {
      s"(${wkt.mkString(",")})"
    }
  }

  def toWKT2: String = {
    val exterior = toLine()
    val interior = innerComponents.map(inner => inner.toLine(true))
    val wkt = List(exterior) ++ interior

    s"${id}\tPOLYGON ( ${wkt.mkString(", ")} )"
  }

  private def getCoordinates(): Array[Coordinate] = {
    var coords = ArrayBuffer.empty[Coordinate]
    var h = outerComponent
    if(h != null){
      coords += new Coordinate(h.v1.x, h.v1.y)
      do{
        coords += new Coordinate(h.v2.x, h.v2.y)
        h = h.next
      }while(h != outerComponent)
    }
    coords.toArray
  }

  def toPolygon(): Polygon = {
    val coords = getCoordinates()
    geofactory.createPolygon(coords)
  }

  def getPolygons(): Vector[Polygon] = {
    toPolygon +: innerComponents.map(_.toPolygon)
  }

  def getGeometry: (Geometry, String) = {
    val polys = getPolygons
    val geom = if(polys.size == 1){
      polys.head // Return a polygon...
    } else {
      // Groups polygons if one contains another...

      val pairs = for{
        outer <- polys
        inner <- polys
      } yield (outer, inner)

      val partial = pairs.filter{ case (outer, inner) => inner.coveredBy(outer) }
        .groupBy(_._1).mapValues(_.map(_._2))
      val diff = partial.values.flatMap(_.tail).toSet
      val result = partial.filterKeys(partial.keySet.diff(diff)).values

      val polygons = result.map{ row =>
        val outerPoly = row.head
        val innerPolys = geofactory.createMultiPolygon(row.tail.toArray)
        outerPoly.difference(innerPolys).asInstanceOf[Polygon]
      }

      polygons.size match {
        // Return an empty polygon...
        case 0 => geofactory.createPolygon(Array.empty[Coordinate])
        // Return a polygon with holes...
        case 1 => polygons.head
        // Return a multi-polygon...
        case x if x > 1 => geofactory.createMultiPolygon(polygons.toArray)
      }
    }
    geom match {
      case geom if geom.isInstanceOf[Polygon] =>{
        val poly = geom.asInstanceOf[Polygon]
        if(poly.getNumInteriorRing > 0){
          (geom, "Polygon with holes")
        } else {
          if(poly.isEmpty()){
            (geom, "Empty")
          } else {
            (geom, "Polygon")
          }
        }
      }
      case g if g.isInstanceOf[MultiPolygon] => (g, "MultiPolygon")
    }
  }

  def toWKT: String = ???
  
  def toWKTperComponent(): String = {
    getPolygons.map(polygon => s"$id\t${polygon.toText()}\n").mkString
  }

  def getEnvelope(): Geometry = {
    geofactory.createGeometryCollection(getPolygons.map(_.getEnvelope).toArray).getEnvelope
  }

  override def compare(that: Face): Int = {
    if(id == that.id){
      getLeftmostVertex.compare(that.getLeftmostVertex)
    } else {
      id.compare(that.id)
    }
  }

  def canEqual(a: Any) = a.isInstanceOf[Face]

  override def equals(that: Any): Boolean =
    that match {
      case that: Face => {
        that.canEqual(this) &&
        this.id == that.id 
       }
      case _ => false
    }

  override def hashCode: Int = {
    val prime = 31
    var result = 1
    val x = Try(this.id.toInt) match {
      case Success(i) => i
      case _ => 0
    }
    result = prime * x
    result = prime * result + (if (id == null) 0 else id.hashCode)
    result
  }
}

case class Edge(v1: Vertex, v2: Vertex, var label: String = "", id: String = "") extends Ordered[Edge] {
  var l = id
  var r = "*"

  override def compare(that: Edge): Int = {
    if (v2.x == that.v2.x) v2.y compare that.v2.y
    else v2.x compare that.v2.x
  }

  def canEqual(a: Any) = a.isInstanceOf[Edge]

  override def equals(that: Any): Boolean =
    that match {
      case that: Edge => {
        that.canEqual(this) &&
        this.v1.equals(that.v1) && 
        this.v2.equals(that.v2) &&
        this.id.equals(that.id)
      }
      case _ => false
    }

  override def hashCode: Int = {
    val prime = 31
    var result = 1
    result = prime * result + v1.x.toInt
    result = prime * result + v2.y.toInt
    result = prime * result + (if (id == null) 0 else id.hashCode)
    result
  }

  def left: String = label.split("<br>")(0)

  def right: String = label.split("<br>")(1)

  override def toString = s"${id} ${v1.toString} ${v2.toString}\t${l} ${r}"

  def toWKT: String = s"${id}\tLINESTRING(${v1.x} ${v1.y}, ${v2.x} ${v2.y})\t${label}"
}


