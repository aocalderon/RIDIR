class Half_edge(){
  private var _id: Long = -1L
  var origen: Vertex = null
  var next: Half_edge = null
  var prev: Half_edge = null
  var twin: Half_edge = null
  var face: Face = null

  def getId = _id

  def setId(id: Long): Unit = {
    _id = id
  }

  def toWKT: String = s"LINESTRING (${origen.x} ${origen.y} , ${twin.origen.x} ${twin.origen.y})"
}

class Vertex(var x: Double, var y: Double){
  private var _id: Double = -1
  var edge: Half_edge = null

  def getId = _id

  def setId(id: Long): Unit = {
    _id = id
  }

  def toWKT: String = s"${getId}\tPOINT ($x $y)\t${edge.getId}"
}

class Face(){
  private var _id: Double = -1
  var outerComponent: Half_edge = null
  var innerComponent: Half_edge = null

  def getId = _id

  def setId(id: Long): Unit = {
    _id = id
  }
}
