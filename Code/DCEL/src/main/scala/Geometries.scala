import scala.collection.mutable.ListBuffer

case class Half_edge() extends Ordered[Half_edge] {
  private var _id: Long = -1L
  var origen: Vertex = null
  var v2: Vertex = null
  var next: Half_edge = null
  var prev: Half_edge = null
  var twin: Half_edge = null
  var face: Face = null

  def getId = _id

  def setId(id: Long): Unit = {
    _id = id
  }

  override def compare(that: Half_edge): Int = {
    if (origen.x == that.origen.x) origen.y compare that.origen.y
    else origen.x compare that.origen.x
  }

  def toWKT: String = s"LINESTRING (${origen.x} ${origen.y} , ${twin.origen.x} ${twin.origen.y})"
}

case class Vertex(x: Double, y: Double) extends Ordered[Vertex] {
  private var _id: Double = -1
  var edge: Half_edge = null
  var half_edges: ListBuffer[Half_edge] = new ListBuffer[Half_edge]()

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

  def toWKT: String = s"${getId}\tPOINT ($x $y)"
}

case class Face(var id: Long){
  var outerComponent: Half_edge = null
  var innerComponent: Half_edge = null
}

case class Edge(v1: Int, v2: Int) extends Ordered[Edge] {
  override def compare(that: Edge): Int = {
    if (v1 == that.v1) v2 compare that.v2
    else v1 compare that.v1
  }
}

case class Edge2(v1: Vertex, v2: Vertex) {
  def toWKT: String = s"LINESTRING(${v1.x} ${v1.y}, ${v2.x} ${v2.y})"
}
