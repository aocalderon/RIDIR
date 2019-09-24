import scala.collection.JavaConverters._
import scala.collection.mutable.{ListBuffer, HashSet, ArrayBuffer}
import com.vividsolutions.jts.geom.GeometryFactory
import com.vividsolutions.jts.geom.{Coordinate, LinearRing, Polygon}

class GraphEdge(pts: Array[Coordinate], hedge: Half_edge) extends com.vividsolutions.jts.geomgraph.Edge(pts) {
  def getVerticesSet: List[Vertex] = {
    var vertices = new ArrayBuffer[Vertex]()
    vertices += hedge.v1
    super.getEdgeIntersectionList().iterator().asScala.toList.foreach{ n =>
      val inter = n.asInstanceOf[com.vividsolutions.jts.geomgraph.EdgeIntersection]
      val coord = inter.getCoordinate
      val index = inter.getSegmentIndex
      vertices += Vertex(coord.x, coord.y)
    }
    vertices += hedge.v2
    vertices.toList.distinct
  }

  def getHalf_edges: List[Half_edge] = {
    var half_edges = new ArrayBuffer[Half_edge]()
    val vertices = this.getVerticesSet
    val segments = vertices.zip(vertices.tail)
    for(segment <- segments){
      val he = Half_edge(segment._1, segment._2)
      he.tag = this.hedge.tag
      he.label = this.hedge.label
      half_edges += he
    }
    half_edges.toList.distinct
  }

  def getVerticesAndIncidents: List[(Half_edge, Vertex)] = {
    this.getHalf_edges.map(hedge => (hedge, hedge.v1)).toList
  }

  def toWKT: String = {
    val intersections = super.getEdgeIntersectionList()
    intersections.iterator().asScala.toList.map{ n =>
      val inter = n.asInstanceOf[com.vividsolutions.jts.geomgraph.EdgeIntersection]
      val coord = inter.getCoordinate
      val seg = inter.getSegmentIndex
      val dist = inter.getDistance
      s"POINT(${coord.x} ${coord.y})\t$seg\t$dist\t${hedge.tag}${hedge.label}"
    }.mkString("\n")
  }
}

case class LocalDCEL(half_edges: List[Half_edge], faces: List[Face], vertices: List[Vertex], edges: List[Edge] = null) {
  var id: Long = -1L
  var tag: String = ""
}

case class MergedDCEL(half_edges: List[Half_edge], faces: List[Face], vertices: List[Vertex], partition: Int = -1, edges: Set[Edge] = null, source: List[Half_edge] = null) {
  
  def union(): List[Face] = {
    faces.filter(_.area() > 0)
  }

  def intersection(): List[Face] = {
    faces.filter(_.tag.split(" ").size == 2)
  }

  def symmetricDifference(): List[Face] = {
    faces.filter(_.tag.split(" ").size == 1)
    .filter(_.area() > 0)
  }

  def differenceA(): List[Face] = {
    symmetricDifference().filter(_.tag.substring(0, 1) == "A")
  }

  def differenceB(): List[Face] = {
    symmetricDifference().filter(_.tag.substring(0, 1) == "B")
  }
}

case class Half_edge(v1: Vertex, v2: Vertex) extends Ordered[Half_edge] {
  var id: String = ""
  var origen: Vertex = v2
  var twin: Half_edge = null
  var next: Half_edge = null
  var prev: Half_edge = null
  var face: Face = null
  var label: String = null
  var tag: String = ""
  var visited: Boolean = false

  val angle  = hangle(v2.x - v1.x, v2.y - v1.y)
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

  def toWKT: String = s"LINESTRING (${twin.origen.x} ${twin.origen.y} , ${origen.x} ${origen.y})\t${tag}${label}"

  def toWKT2: String = s"LINESTRING (${v2.x} ${v2.y} , ${v1.x} ${v1.y})\t${tag}${label}"

  def toWKT3: String = s"LINESTRING (${v1.x} ${v1.y} , ${v2.x} ${v2.y})"
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

  def getHalf_edges(): List[Half_edge] = {
    this.half_edges.toList
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

  override def toString = s"($x, $y)"

  def toWKT: String = s"POINT ($x $y)"
}

case class Face(label: String){
  private val geofactory: GeometryFactory = new GeometryFactory();
  var id: String = ""
  var outerComponent: Half_edge = null
  var innerComponent: List[Face] = List.empty[Face]
  var exterior: Boolean = false
  var tag: String = ""
  var nHalf_edges = 0

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

  def area(): Double = {
    val boundary = faceArea()
    val holes = innerComponent.map(_.faceArea()).sum
    boundary + holes
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

  def toWKT(): String = {
    if(area() <= 0){
      s"${id}\tPOLYGON EMPTY\t${tag}${label}"
    } else {
      var hedge = outerComponent
      var wkt = new ArrayBuffer[String]()
      wkt += s"${hedge.v1.x} ${hedge.v1.y}"
      while(hedge.next != outerComponent){
        wkt += s"${hedge.v2.x} ${hedge.v2.y}"
        hedge = hedge.next
      }
      wkt += s"${hedge.v2.x} ${hedge.v2.y}"
    
      s"${id}\tPOLYGON (( ${wkt.mkString(" , ")} ))\t${tag}${label}"
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
    if(id == "*"){
      s"${id}\tPOLYGON EMPTY\t${tag}${label}"
    } else {
      val exterior = toLine() 
      val interior = innerComponent.map(inner => inner.toLine(true)).mkString(" , ")

      s"${id}\tPOLYGON ( $exterior , $interior )\t${tag}${label}"
    }
  }

  def toPolygon(): Polygon = {
    var coords = ArrayBuffer.empty[Coordinate]
    if(area() > 0){
      var hedge = outerComponent
      coords += new Coordinate(hedge.v1.x, hedge.v1.y)
      while(hedge.next != outerComponent){
        coords += new Coordinate(hedge.v2.x, hedge.v2.y)
        hedge = hedge.next
      }
      coords += new Coordinate(hedge.v2.x, hedge.v2.y)
    }
    val ring = geofactory.createLinearRing(coords.toArray)
    geofactory.createPolygon(ring)
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


/////////////////////////////

case class Half_edge3() extends Ordered[Half_edge3] {
  private var _id: Long = -1L
  var origen: Vertex3 = null
  var next: Half_edge3 = null
  var prev: Half_edge3 = null
  var twin: Half_edge3 = null
  var face: Face3 = null

  def getId = _id

  def setId(id: Long): Unit = {
    _id = id
  }

  override def compare(that: Half_edge3): Int = {
    if (origen.x == that.origen.x) origen.y compare that.origen.y
    else origen.x compare that.origen.x
  }

  def toWKT: String = s"LINESTRING (${origen.x} ${origen.y} , ${twin.origen.x} ${twin.origen.y})"
}

case class Vertex3(x: Double, y: Double) extends Ordered[Vertex3] {
  private var _id: Double = -1
  var edge: Half_edge3 = null

  def getId = _id

  def setId(id: Long): Unit = {
    _id = id
  }

  override def compare(that: Vertex3): Int = {
    if (x == that.x) y compare that.y
    else x compare that.x
  }

  def toWKT: String = s"${getId}\tPOINT ($x $y)"
}

case class Face3(var id: Long){
  var outerComponent: Half_edge3 = null
  var innerComponent: Half_edge3 = null
}
