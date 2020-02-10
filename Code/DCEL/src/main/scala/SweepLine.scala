import org.slf4j.{LoggerFactory, Logger}
import org.rogach.scallop._
import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.spatialRDD.{SpatialRDD, PolygonRDD, LineStringRDD}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import com.vividsolutions.jts.operation.buffer.BufferParameters
import com.vividsolutions.jts.geom.GeometryFactory
import com.vividsolutions.jts.geom.{Geometry, Envelope, Coordinate,  Polygon, LinearRing, LineString}
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence
import com.vividsolutions.jts.geomgraph.index.{SimpleMCSweepLineIntersector, SegmentIntersector}
import com.vividsolutions.jts.algorithm.RobustLineIntersector
import com.vividsolutions.jts.io.WKTReader
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object SweepLine{
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val geofactory: GeometryFactory = new GeometryFactory();
  private val reader = new WKTReader(geofactory)
  private val precision: Double = 0.001
  private val startTime: Long = 0L

  def clocktime = System.currentTimeMillis()

  def log(msg: String, timer: Long, n: Long, status: String): Unit ={
    logger.info("DCEL|%6.2f|%-50s|%6.2f|%6d|%s".format((clocktime-startTime)/1000.0, msg, (clocktime-timer)/1000.0, n, status))
  }

  def computeIntersections(edgesA: List[GraphEdge], edgesB: List[GraphEdge]): List[Half_edge] = {
    val gedges1 = edgesA.asJava
    val gedges2 = edgesB.asJava
    val sweepline = new SimpleMCSweepLineIntersector()
    val lineIntersector = new RobustLineIntersector()
    val segmentIntersector = new SegmentIntersector(lineIntersector, true, true)
    sweepline.computeIntersections(gedges1, gedges2, segmentIntersector)

    val g1 = gedges1.asScala.flatMap(_.getHalf_edges)
    val g2 = gedges2.asScala.flatMap(_.getHalf_edges)
    
    g1.union(g2).toList.groupBy(g => g).map{ case (k, v) =>
      val label = v.map(x => s"${x.tag}${x.label}").mkString(" ")
      k.label = label
      k
    }.toList
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
