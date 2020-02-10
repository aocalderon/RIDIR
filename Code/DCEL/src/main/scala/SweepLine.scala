import com.vividsolutions.jts.geomgraph.index.{SimpleMCSweepLineIntersector, SegmentIntersector}
import com.vividsolutions.jts.algorithm.RobustLineIntersector
import scala.collection.JavaConverters._

object SweepLine{

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
}
