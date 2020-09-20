import com.vividsolutions.jts.geomgraph.index.{SimpleMCSweepLineIntersector, SegmentIntersector}
import com.vividsolutions.jts.algorithm.RobustLineIntersector
import com.vividsolutions.jts.geom.{PrecisionModel, Envelope, Coordinate}
import com.vividsolutions.jts.geom.GeometryFactory
import com.vividsolutions.jts.geomgraph.EdgeIntersection
import com.vividsolutions.jts.geom.{LineString, Polygon}
import scala.collection.JavaConverters._

object SweepLine{

  def getEdgesOnCell(edges: Vector[LineString], cell: Envelope, precision: Double = 0.01)
      : Vector[Coordinate] = {

    cell.expandBy(-precision)
    val edgesList = edges.filter{ edge =>
      edge.getCoordinates.exists(coord => !cell.contains(coord))
    }.map{ linestring =>
      new com.vividsolutions.jts.geomgraph.Edge(linestring.getCoordinates)
    }.asJava

    println
    edgesList.asScala.foreach{println}
    println
    println(envelope2polygon(cell).toText)

    //cell.expandBy(precision)
    val e = cell.getMaxX
    val w = cell.getMinX
    val n = cell.getMaxY
    val s = cell.getMinY
    val EN = new Coordinate(e, n)
    val WN = new Coordinate(w, n)
    val ES = new Coordinate(e, s)
    val WS = new Coordinate(w, s)

    val W = new com.vividsolutions.jts.geomgraph.Edge(Array(WN, WS))
    val S = new com.vividsolutions.jts.geomgraph.Edge(Array(WS, ES))
    val E = new com.vividsolutions.jts.geomgraph.Edge(Array(ES, EN))
    val N = new com.vividsolutions.jts.geomgraph.Edge(Array(EN, WN))
    val cellList = List(W,S,E,N).asJava

    val sweepline = new SimpleMCSweepLineIntersector()
    val lineIntersector = new RobustLineIntersector()
    val segmentIntersector = new SegmentIntersector(lineIntersector, true, true)

    sweepline.computeIntersections(edgesList, cellList, segmentIntersector)
 
    cellList.asScala.flatMap{ edge =>
      edge.getEdgeIntersectionList.iterator.asScala.map{ i =>
        i.asInstanceOf[EdgeIntersection].getCoordinate
      }
    }.toVector
  }

  def edge2wkt(edge: com.vividsolutions.jts.geomgraph.Edge): String = {
    val model = new PrecisionModel(1000)
    val geofactory = new GeometryFactory(model)
    val coords = edge.getCoordinates
    geofactory.createLineString(coords).toText
  }

  def computeIntersections(edgesA: List[GraphEdge], edgesB: List[GraphEdge]): (List[GraphEdge], List[GraphEdge]) = {
    val gedges1 = edgesA.asJava
    val gedges2 = edgesB.asJava
    val sweepline = new SimpleMCSweepLineIntersector()
    val lineIntersector = new RobustLineIntersector()
    val segmentIntersector = new SegmentIntersector(lineIntersector, true, true)
    sweepline.computeIntersections(gedges1, gedges2, segmentIntersector)
    val g1 = gedges1.asScala.toList
    val g2 = gedges2.asScala.toList

    (g1, g2)
  }

  def getGraphEdgeIntersections(edgesA: List[GraphEdge], edgesB: List[GraphEdge]): List[GraphEdge] = {
    val gedges1 = edgesA.asJava
    val gedges2 = edgesB.asJava
    val sweepline = new SimpleMCSweepLineIntersector()
    val lineIntersector = new RobustLineIntersector()
    val segmentIntersector = new SegmentIntersector(lineIntersector, true, true)
    sweepline.computeIntersections(gedges1, gedges2, segmentIntersector)

    val g1 = gedges1.asScala
    val g2 = gedges2.asScala
    
    g1.union(g2).toList
  }

  def getGraphEdgeIntersectionsOnB(edgesA: List[GraphEdge], edgesB: List[GraphEdge]): List[GraphEdge] = {
    val gedges1 = edgesA.asJava
    val gedges2 = edgesB.asJava
    val sweepline = new SimpleMCSweepLineIntersector()
    val lineIntersector = new RobustLineIntersector()
    val segmentIntersector = new SegmentIntersector(lineIntersector, true, true)
    sweepline.computeIntersections(gedges1, gedges2, segmentIntersector)

    gedges2.asScala.toList
  }

  def envelope2polygon(e: Envelope): Polygon = {
    val model = new PrecisionModel(1000)
    val geofactory = new GeometryFactory(model)
    val minX = e.getMinX()
    val minY = e.getMinY()
    val maxX = e.getMaxX()
    val maxY = e.getMaxY()
    val p1 = new Coordinate(minX, minY)
    val p2 = new Coordinate(maxX, minY)
    val p3 = new Coordinate(maxX, maxY)
    val p4 = new Coordinate(minX, maxY)
    geofactory.createPolygon(Array(p1,p2,p3,p4,p1))
  }

  def main(args: Array[String]) = {
    val sweepline = new SimpleMCSweepLineIntersector()
    val lineIntersector = new RobustLineIntersector()
    val model = new PrecisionModel(1000)
    lineIntersector.setPrecisionModel(model)
    val segmentIntersector = new SegmentIntersector(lineIntersector, true, true)
    
    import scala.io.Source
    import com.vividsolutions.jts.io.WKTReader
    val geofactory = new GeometryFactory(model)
    val reader = new WKTReader(geofactory)
    val home = System.getProperty("user.home")

    /********************************************************************/
    val edgesBuff = Source.fromFile(s"${home}/RIDIR/Code/Scripts/gadm/edges2.wkt")
    val edges = edgesBuff.getLines.map{ line =>
      val arr = line.split("\t")
      val linestring = reader.read(arr(0)).asInstanceOf[LineString]
      val data = arr.tail.mkString("\t")
      linestring.setUserData(data)
      linestring
    }.toVector
    edgesBuff.close
    val cellBuff = Source.fromFile(s"${home}/RIDIR/Code/Scripts/gadm/cell.wkt")
    val cellEnvelope = cellBuff.getLines.map{ line =>
      val cell = reader.read(line)
      cell.getEnvelopeInternal
    }.toList.head
    cellBuff.close

    val edgesA = SweepLine.getEdgesOnCell(edges, cellEnvelope)
    edgesA.foreach{println}

  }
}
