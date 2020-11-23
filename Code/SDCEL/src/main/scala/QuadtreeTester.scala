import org.slf4j.{LoggerFactory, Logger}
import org.rogach.scallop._
import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.spatialPartitioning.quadtree._
import org.datasyslab.geospark.spatialRDD.{SpatialRDD, PolygonRDD, LineStringRDD}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel}
import com.vividsolutions.jts.geom.{Geometry, Envelope, Coordinate,  Polygon, LinearRing, LineString, MultiPolygon, Point}
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence
import com.vividsolutions.jts.algorithm.{RobustCGAlgorithms, CGAlgorithms}
import com.vividsolutions.jts.io.WKTReader
import org.geotools.geometry.jts.GeometryClipper
import scala.collection.JavaConverters._
import scala.collection.mutable.{ListBuffer, TreeSet, ArrayBuffer, HashSet}

object QuadtreeTester {
def main(arg: Array[String]): Unit = {
  val precisionModel = new PrecisionModel(math.pow(10, 3))
  val geofactory = new GeometryFactory(precisionModel)
  val spark = SparkSession.builder().getOrCreate()
  import spark.implicits._

  // Reading data...
  val input = "/home/acald013/Datasets/CA/cali2000_polygons6414.tsv"
  val offset = 2
  val quote = true
  val polygonRDD = new SpatialRDD[Polygon]()
  val polygons = spark.read.textFile(input).repartition(200).rdd.zipWithUniqueId().map{ case (line, i) =>
    val arr = line.split("\t")
    val userData = List(s"$i") ++ (0 until arr.size).filter(_ != offset).map(i => arr(i))
    var wkt = arr(offset)
    if(quote){
      wkt = wkt.replaceAll("\"", "")
    }
    val polygon = new WKTReader(geofactory).read(wkt)
    polygon.setUserData(userData.mkString("\t"))
    polygon.asInstanceOf[Polygon]
  }.cache
  polygonRDD.setRawSpatialRDD(polygons)
  polygonRDD.analyze()

  // Partition edges...
  def getRings(polygon: Polygon): List[Array[Coordinate]] = {
    var exteriorCoords = polygon.getExteriorRing.getCoordinateSequence.toCoordinateArray()
    if(!CGAlgorithms.isCCW(exteriorCoords)) { exteriorCoords = exteriorCoords.reverse }
    val outerRing = List(exteriorCoords)

    val nInteriorRings = polygon.getNumInteriorRing
    val innerRings = (0 until nInteriorRings).map{ i =>
      var interiorCoords = polygon.getInteriorRingN(i).getCoordinateSequence.toCoordinateArray()
      if(CGAlgorithms.isCCW(interiorCoords)) { interiorCoords = interiorCoords.reverse }
      interiorCoords
    }.toList

    outerRing ++ innerRings
  }

  def getHalf_edges(polygon: Polygon): List[LineString] = {
    val polygon_id = polygon.getUserData.toString().split("\t")(0)
    val rings = getRings(polygon).zipWithIndex
    val hasHoles = rings.size > 1 match {
      case true => 1
      case _    => 0
    }
    rings.flatMap{ ring =>
      val ring_id = ring._2
      ring._1.zip(ring._1.tail).zipWithIndex.map{ pair =>
        val order = pair._2
        val coord1 = pair._1._1
        val coord2 = pair._1._2
        val coords = new CoordinateArraySequence(Array(coord1, coord2))

        val line = geofactory.createLineString(coords)
        line.setUserData(s"${polygon_id}\t${ring_id}\t${order}\t${hasHoles}")

        line
      }
    }
  }

  val edges = polygons.map(_.asInstanceOf[Polygon]).flatMap(getHalf_edges).cache
  val nEdges = edges.count()

  val edgesRDD = new SpatialRDD[LineString]()
  edgesRDD.setRawSpatialRDD(edges)
  edgesRDD.analyze()
  val entries = 200
  val levels = 8
  val fraction = 0.1
  val boundary = new QuadRectangle(edgesRDD.boundaryEnvelope)
  val quadtree = new StandardQuadTree[LineString](boundary, 0, entries, levels)
  for(edge <- edges.sample(false, fraction, 42).collect()) {
    quadtree.insert(new QuadRectangle(edge.getEnvelopeInternal), edge)
  }
  quadtree.assignPartitionIds()
  quadtree.assignPartitionLineage()
  val EdgePartitioner = new QuadTreePartitioner(quadtree)
  edgesRDD.spatialPartitioning(EdgePartitioner)
  edgesRDD.spatialPartitionedRDD.rdd.cache
  val cells = quadtree.getLeafZones.asScala.map(c => c.partitionId -> c.getEnvelope).toMap

  def envelope2Polygon(e: Envelope): Polygon = {
    val minX = e.getMinX()
    val minY = e.getMinY()
    val maxX = e.getMaxX()
    val maxY = e.getMaxY()
    val p1 = new Coordinate(minX, minY)
    val p2 = new Coordinate(maxX, minY)
    val p3 = new Coordinate(maxX, maxY)
    val p4 = new Coordinate(minX, maxY)
    val ring = geofactory.createLinearRing(Array(p1,p2,p3,p4,p1))
    geofactory.createPolygon(ring)
  }

  val linageById = quadtree.getLeafZones.asScala.map(z => (z.lineage -> z.partitionId)).toMap
  val zones = quadtree.getLeafZones.asScala.map(c => c.partitionId -> c).toMap
  def getSiblingLinage(l: String): List[String] = {
    val size_1 = l.size - 1
    val linage = l.substring(0, size_1)
    val parent = linage.reverse.head
    linage.head match {
      case '0' => List(s"${parent}1", s"${parent}2", s"${parent}3")//.map(_.toInt)
      case '1' => List(s"${parent}0", s"${parent}2", s"${parent}3")//.map(_.toInt)
      case '2' => List(s"${parent}0", s"${parent}1", s"${parent}3")//.map(_.toInt)
      case '3' => List(s"${parent}0", s"${parent}1", s"${parent}2")//.map(_.toInt)
      case _   => List.empty[String]
    }
  }
  def getSiblingIDs(id: Int): List[Int] = {
    println(s"Looking for $id")
    val linage = zones.get(id).get.lineage
    getSiblingLinage(linage).map(l => linageById.get(l).get.toInt)
  }


  edgesRDD.spatialPartitionedRDD.rdd.mapPartitionsWithIndex{ case (index, partition) =>
    val cell = envelope2Polygon(cells.get(index).get)
    val zone = zones.get(index).get
    List( (index, partition.filter(_.coveredBy(cell)).size, zone) ).toIterator
  }.filter(_._2 == 0)
  .map{ z =>
    s"Cell ${z._1}: ${z._3.lineage} => ${getSiblingIDs(z._1).mkString(", ")}"
  }
  .foreach(println)
}
}

