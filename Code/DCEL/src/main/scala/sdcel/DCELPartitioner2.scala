import scala.collection.JavaConverters._
import com.vividsolutions.jts.geom.{Coordinate, Envelope}
import com.vividsolutions.jts.geom.{LineString, Polygon}
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.io.WKTReader
import com.vividsolutions.jts.algorithm.CGAlgorithms
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.slf4j.{Logger, LoggerFactory}
import edu.ucr.dblab.quadtree._

object DCELPartitioner{
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]) = {
    // Starting session...
    logger.info("Starting session...")
    implicit val spark = SparkSession.builder()
        .config("spark.serializer",classOf[KryoSerializer].getName)
        .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
        .getOrCreate()
    import spark.implicits._
    implicit val model = new PrecisionModel(1000)
    implicit val geofactory = new GeometryFactory(model)
    //val input = "gadm/level1"
    val input = "tmp/polyDemo.wkt"
    val partitions = 4
    val precision = 1.0 / model.getScale
    logger.info(s"Using $precision precision.")
    logger.info("Starting session... Done!")

    // Reading data...
    val edgesRaw = spark.read.textFile(input).rdd.zipWithUniqueId
      .mapPartitionsWithIndex{ case(index, lines) =>
        val reader = new WKTReader(geofactory)
        lines.flatMap{ case(wkt, id) =>
          val polygon = reader.read(wkt).asInstanceOf[Polygon]
          getLineStrings(polygon, id)
        }.toIterator
      }.persist()
    val nEdgesRDD = edgesRaw.count()
    val edgesRDD = new SpatialRDD[LineString]()
    edgesRDD.setRawSpatialRDD(edgesRaw)
    edgesRDD.analyze()

    //
    val fc = new FractionCalculator()
    val fraction = fc.getFraction(partitions, nEdgesRDD)
    logger.info(s"Fraction: ${fraction}")
    val samples = edgesRaw.sample(false, fraction, 42)
      .map(_.getEnvelopeInternal).collect().toList.asJava
    val partitioning = new QuadtreePartitioning(samples,
      edgesRDD.boundary(), partitions)
    val quadtree = partitioning.getPartitionTree()
    quadtree.assignPartitionLineage()
    val partitioner = new QuadTreePartitioner(quadtree)
    edgesRDD.spatialPartitioning(partitioner)
    val edges = edgesRDD.spatialPartitionedRDD.rdd.persist()

    // Getting cells...
    case class Cell(id: Int, lineage: String, mbr: Polygon)
    val cells = quadtree.getLeafZones.asScala.map{ leaf =>
      val id = leaf.partitionId.toInt
      val lineage = leaf.lineage
      val mbr = envelope2polygon(roundEnvelope(leaf.getEnvelope, model.getScale))

      val cell = Cell(id, lineage, mbr)
      (id -> cell)
    }.toMap
    save{"/tmp/edgesCells.wkt"}{
      cells.values.map{ cell =>
        val wkt = cell.mbr.toText
        val id = cell.id
        val lineage = cell.lineage
        s"$wkt\t$id\t$lineage\n"
      }.toList
    }
    
    // Saving to HDFS...
    val sample = edges.mapPartitionsWithIndex{ (index, edgesIt) =>
      edgesIt.map{ edge =>
        val wkt = edge.toText
        val data = edge.getUserData

        s"$wkt\t$index\t$data"
      }
    }.toDF.write
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .text("tmp/edgesDemo")

    spark.close
  }

  def getLineStrings(polygon: Polygon, polygon_id: Long)(implicit geofactory: GeometryFactory): List[LineString] = {
    getRings(polygon).zipWithIndex
      .flatMap{ case(ring, ring_id) =>
        ring.zip(ring.tail).zipWithIndex.map{ case(pair, order) =>
          val coord1 = pair._1
          val coord2 = pair._2
          val coords = Array(coord1, coord2)
          val isHole = ring_id > 0

          val line = geofactory.createLineString(coords)
          line.setUserData(s"$polygon_id\t$ring_id\t$order\t${isHole}")

          line
        }
    }
  }

  private def getRings(polygon: Polygon): List[Array[Coordinate]] = {
    val ecoords = polygon.getExteriorRing.getCoordinateSequence.toCoordinateArray()
    val outerRing = if(!CGAlgorithms.isCCW(ecoords)) { ecoords.reverse } else { ecoords }
    
    val nInteriorRings = polygon.getNumInteriorRing
    val innerRings = (0 until nInteriorRings).map{ i => 
      val icoords = polygon.getInteriorRingN(i).getCoordinateSequence.toCoordinateArray()
      if(CGAlgorithms.isCCW(icoords)) { icoords.reverse } else { icoords }
    }.toList

    outerRing +: innerRings
  }

  def envelope2polygon(e: Envelope)(implicit geofactory: GeometryFactory): Polygon = {
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

  def roundEnvelope(envelope: Envelope, scale: Double): Envelope = {
    val e = round(envelope.getMinX, scale)
    val w = round(envelope.getMaxX, scale)
    val s = round(envelope.getMinY, scale)
    val n = round(envelope.getMaxY, scale)
    new Envelope(e, w, s, n)
  }
  private def round(number: Double, scale: Double): Double =
    Math.round(number * scale) / scale

  def save(filename: String)(content: Seq[String]): Unit = {
    val start = clocktime
    val f = new java.io.PrintWriter(filename)
    f.write(content.mkString(""))
    f.close
    val end = clocktime
    val time = "%.2f".format((end - start) / 1000.0)
    logger.info(s"Saved ${filename} in ${time}s [${content.size} records].")
  }
  private def clocktime = System.currentTimeMillis()
}
