import scala.collection.mutable.{ListBuffer, HashSet, ArrayBuffer}

case class Half_edge(v1: Vertex, v2: Vertex) extends Ordered[Half_edge] {
  private var _id: Long = -1L
  var origen: Vertex = v2
  var twin: Half_edge = null
  var next: Half_edge = null
  var prev: Half_edge = null
  var face: Face = null

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

  def toWKT: String = s"LINESTRING (${origen.x} ${origen.y} , ${twin.origen.x} ${twin.origen.y})"
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

case class Face(var id: Long){
  var outerComponent: Half_edge = null
  var innerComponent: Half_edge = null
  var data: String = null

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

  def perimeterTest(): String = {
    var p = new ArrayBuffer[Double]()
    var h = outerComponent
    while(h.next != outerComponent){
      p += h.length
      h = h.next
    }
    p += h.length
    p.mkString(" ")
  }

  def toWKT(): String = {
    if(area() <= 0){
      s"POLYGON EMPTY\t-1"
    } else {
      var hedge = outerComponent
      var wkt = new ArrayBuffer[String]()
      wkt += s"${hedge.v1.x} ${hedge.v1.y}"
      while(hedge.next != outerComponent){
        wkt += s"${hedge.v2.x} ${hedge.v2.y}"
        hedge = hedge.next
      }
      wkt += s"${hedge.v2.x} ${hedge.v2.y}"
    
      s"POLYGON (( ${wkt.mkString(" , ")} ))\t${id}"
    }
  }
}

case class Edge(v1: Int, v2: Int) extends Ordered[Edge] {
  override def compare(that: Edge): Int = {
    if (v1 == that.v1) v2 compare that.v2
    else v1 compare that.v1
  }
}

case class Edge2(v1: Vertex, v2: Vertex) {
  var left  = ""
  var right = ""

  def canEqual(a: Any) = a.isInstanceOf[Edge2]

  override def equals(that: Any): Boolean =
    that match {
      case that: Edge2 => {
        that.canEqual(this) &&
          ( (this.v1.equals(that.v1) && this.v2.equals(that.v2)) ||
            (this.v1.equals(that.v2) && this.v2.equals(that.v1)) )
      }
      case _ => false
    }


  def toWKT: String = s"LINESTRING(${v1.x} ${v1.y}, ${v2.x} ${v2.y})"
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
