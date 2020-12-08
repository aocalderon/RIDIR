import com.vividsolutions.jts.geomgraph.index.{SimpleMCSweepLineIntersector, SegmentIntersector}
import com.vividsolutions.jts.algorithm.RobustLineIntersector
import com.vividsolutions.jts.geom.{PrecisionModel, Envelope, Coordinate}
import com.vividsolutions.jts.geom.GeometryFactory
import com.vividsolutions.jts.geomgraph.EdgeIntersection
import com.vividsolutions.jts.geom.{LinearRing, LineString, Polygon}
import scala.collection.JavaConverters._
import edu.ucr.dblab.sdcel.geometries.EdgeData

object SweepLine{

  def getEdgesOnCell(outerEdges: Vector[LineString], mbr: LinearRing)
    (implicit geofactory: GeometryFactory)
      : Vector[LineString] = {

    val edgesList = outerEdges.map{ linestring =>
      new com.vividsolutions.jts.geomgraph.Edge(linestring.getCoordinates)
    }.asJava

    val mbrList = mbr.getCoordinates.zip(mbr.getCoordinates.tail)
      .toList.map{ case(p1, p2) =>
        new com.vividsolutions.jts.geomgraph.Edge(Array(p1, p2))
      }.asJava

    val sweepline = new SimpleMCSweepLineIntersector()
    val lineIntersector = new RobustLineIntersector()
    val segmentIntersector = new SegmentIntersector(lineIntersector, true, true)

    sweepline.computeIntersections(edgesList, mbrList, segmentIntersector)

    val edgesOnCell = mbrList.asScala.flatMap{ edge =>
      val extremes = edge.getCoordinates
      val start = extremes(0)
      val end = extremes(1)
      val inners = edge.getEdgeIntersectionList.iterator.asScala.map{ i =>
        i.asInstanceOf[EdgeIntersection].getCoordinate
      }.toArray

      val coords = start +: inners :+ end
      coords.zip(coords.tail).map{ case(p1, p2) =>
        val edge = geofactory.createLineString(Array(p1, p2))
        edge.setUserData(EdgeData(-1,0,0,false))
        edge
      }
    }.toVector

    val outerEdgesMap = outerEdges.map{ edge => edge.getCoordinates -> edge }.toMap
    val envelope = mbr.getEnvelopeInternal
    val edgesInsideCell = edgesList.asScala.flatMap{ edge =>
      val extremes = edge.getCoordinates
      val data = outerEdgesMap(extremes).getUserData
      val start = extremes(0)
      val end = extremes(1)

      val eiList = edge.getEdgeIntersectionList.iterator.asScala.toList
      if(eiList.length == 1){
        val intersection = eiList.head.asInstanceOf[EdgeIntersection].getCoordinate
        val coords = {
          if(start != intersection &&
            end != intersection &&
            envelope.contains(start) &&
            !envelope.contains(end)) Array(start, intersection)
          else if(start != intersection &&
            end != intersection &&
            !envelope.contains(start) &&
            envelope.contains(end)) Array(intersection, end)
          else if(start == intersection &&
            envelope.contains(end)) Array(intersection, end)
          else if(end == intersection &&
            envelope.contains(start)) Array(start, intersection)
          else Array.empty[Coordinate]
        }
        val segment = geofactory.createLineString(coords)
        segment.setUserData(data)
        Vector(segment)
      } else {
        val eiList = edge.getEdgeIntersectionList
        eiList.addEndpoints()
        val coords = eiList.iterator.asScala.toArray
          .map(_.asInstanceOf[EdgeIntersection].getCoordinate)
        coords.zip(coords.tail).foreach{ case(p1, p2) =>
          val wkt = geofactory.createLineString(Array(p1,p2)).toText
          //println(wkt)
        }
        Vector.empty[LineString]
      }
    }.toVector

    edgesOnCell union edgesInsideCell
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
    implicit val model = new PrecisionModel(1000)
    implicit val geofactory = new GeometryFactory(model)
    val precision = 1.0 / model.getScale
    lineIntersector.setPrecisionModel(model)
    val segmentIntersector = new SegmentIntersector(lineIntersector, true, true)
    
    import scala.io.Source
    import com.vividsolutions.jts.io.WKTReader
    val reader = new WKTReader(geofactory)
    val home = System.getProperty("user.home")

    /********************************************************************/
    val edgesBuff = Source.fromFile(s"${home}/RIDIR/Code/Scripts/gadm/edges.wkt")
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
      val ring = reader.read(line).asInstanceOf[Polygon]
      geofactory.createLinearRing(ring.getCoordinates)
    }.toList.head
    cellBuff.close

    val edgesOnCell = SweepLine.getEdgesOnCell(edges, cellEnvelope)
    edgesOnCell.foreach{println}

  }
}