import scala.collection.JavaConverters._
import scala.collection.mutable.{ListBuffer, HashSet, ArrayBuffer}
import com.vividsolutions.jts.geom.Coordinate

class GraphEdge(pts: Array[Coordinate], hedge: Half_edge) extends com.vividsolutions.jts.geomgraph.Edge(pts) {
  def getVertices: List[Vertex] = {
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
    val vertices = this.getVertices
    val segments = vertices.zip(vertices.tail)
    for(segment <- segments){
      val he = Half_edge(segment._1, segment._2)
      he.tag = this.hedge.tag
      he.label = this.hedge.label
      half_edges += he
    }
    half_edges.toList
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

case class LocalDCEL(half_edges: List[Half_edge], faces: List[Face], vertices: List[Vertex]) {
  var id: Long = -1L
  var tag: String = ""
}

case class Half_edge(v1: Vertex, v2: Vertex) extends Ordered[Half_edge] {
  private var _id: Long = -1L
  var origen: Vertex = v2
  var twin: Half_edge = null
  var next: Half_edge = null
  var prev: Half_edge = null
  var face: Face = null
  var label: String = null
  var tag: String = ""

  val angle  = hangle(v2.x - v1.x, v2.y - v1.y)
  val length = math.sqrt(math.pow(v2.x - v1.x, 2) + math.pow(v2.y - v1.y, 2))

  def getId = _id

  def setId(id: Long): Unit = {
    _id = id
  }

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

  def toWKT: String = s"LINESTRING (${origen.x} ${origen.y} , ${twin.origen.x} ${twin.origen.y})\t${tag}${label}"
  def toWKT2: String = s"LINESTRING (${v1.x} ${v1.y} , ${v1.x} ${v2.y})\t${tag}${label}"
}

case class Vertex(x: Double, y: Double) extends Ordered[Vertex] {
  private var _id: Double = -1
  var edge: Half_edge = null
  var half_edges: HashSet[Half_edge] = new HashSet[Half_edge]()
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

  def toWKT: String = s"${getId}\tPOINT ($x $y)"
}

case class Face(label: String){
  var outerComponent: Half_edge = null
  var innerComponent: Half_edge = null
  var exterior: Boolean = false
  var tag: String = ""

  def area(): Double = {
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

  def toWKT(): String = {
    if(area() <= 0){
      s"POLYGON EMPTY\t${tag}${label}"
    } else {
      var hedge = outerComponent
      var wkt = new ArrayBuffer[String]()
      wkt += s"${hedge.v1.x} ${hedge.v1.y}"
      while(hedge.next != outerComponent){
        wkt += s"${hedge.v2.x} ${hedge.v2.y}"
        hedge = hedge.next
      }
      wkt += s"${hedge.v2.x} ${hedge.v2.y}"
    
      s"POLYGON (( ${wkt.mkString(" , ")} ))\t${tag}${label}"
    }
  }
}

case class Edge(v1: Vertex, v2: Vertex, var label: String = "") extends Ordered[Edge] {
  override def compare(that: Edge): Int = {
    if (v2.x == that.v2.x) v2.y compare that.v2.y
    else v2.x compare that.v2.x
  }

  def canEqual(a: Any) = a.isInstanceOf[Edge]

  override def equals(that: Any): Boolean =
    that match {
      case that: Edge => {
        that.canEqual(this) && ( this.v1.equals(that.v1) && this.v2.equals(that.v2) )
      }
      case _ => false
    }

  def left: String = label.split("<br>")(0)

  def right: String = label.split("<br>")(1)

  def toWKT: String = s"LINESTRING(${v1.x} ${v1.y}, ${v2.x} ${v2.y})\t${label}"
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
