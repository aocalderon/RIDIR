import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import com.vividsolutions.jts.geom.{Envelope, Polygon}
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.spatialPartitioning.quadtree.{QuadTreePartitioner, StandardQuadTree, QuadRectangle}
import org.datasyslab.geospark.spatialPartitioning.{KDBTree, KDBTreePartitioner}
import org.rogach.scallop._
import org.slf4j.{Logger, LoggerFactory}
import DCELBuilder.save
import CellManager.envelope2Polygon

case class Settings(params: SplitterConf, spark: SparkSession)

object DatasetSplitter {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")
  val model: PrecisionModel = new PrecisionModel(1000)
  val geofactory: GeometryFactory = new GeometryFactory(model);
  val precision: Double = 1 / model.getScale

  def readPolygonsA(implicit settings: Settings): (RDD[Polygon], Long) = {
    val input  = settings.params.input1()
    val offset = settings.params.offset1()
    readPolygons(settings.spark: SparkSession, input: String, offset: Int, "A")
  }

  def readPolygonsB(implicit settings: Settings): (RDD[Polygon], Long) = {
    val input  = settings.params.input2()
    val offset = settings.params.offset2()
    readPolygons(settings.spark: SparkSession, input: String, offset: Int, "B")
  }

  def readPolygons(spark: SparkSession, input: String, offset: Int, tag: String): (RDD[Polygon], Long) = {
    val polygons = spark.read.textFile(input).rdd.zipWithUniqueId().map{ case (line, i) =>
      val arr = line.split("\t")
      val userData = s"${tag}${i}" +: (0 until arr.size).filter(_ != offset).map(i => arr(i))
      val wkt =  arr(offset).replaceAll("\"", "")
      val polygon = new WKTReader(geofactory).read(wkt)
      //polygon.setUserData(userData.mkString("\t"))
      polygon.asInstanceOf[Polygon]
    }.cache
    val nPolygons = polygons.count()
    (polygons, nPolygons)
  }

  def getQuadTree(polys: RDD[Polygon], boundary: QuadRectangle)(implicit settings: Settings): StandardQuadTree[Polygon] = {
    val capacity = settings.params.capacity()
    val levels   = settings.params.levels()
    val fraction = settings.params.fraction()

    val quadtree = new StandardQuadTree[Polygon](boundary, 0, capacity, levels)
    val sample = polys.sample(false, fraction, 42)
    for(poly <- sample.collect()) {
      quadtree.insert(new QuadRectangle(poly.getEnvelopeInternal), poly)
    }
    quadtree.assignPartitionIds()
    quadtree.assignPartitionLineage()

    quadtree
  }

  def isValidPoligon(wkt: String): Boolean = {
    val reader = new WKTReader(geofactory)
    val geom = reader.read(wkt)

    geom.isValid()
  }

  /***
   * The main function...
   **/
  def main(args: Array[String]) = {
    val params     = new SplitterConf(args)
    val input1     = params.input1()
    val offset1    = params.offset1()
    val input2     = params.input2()
    val offset2    = params.offset2()
    val partitions = params.partitions()
    val gridType   = params.grid() match {
      case "QUADTREE"  => GridType.QUADTREE
      case "KDBTREE"   => GridType.KDBTREE
    }

    // Starting session...
    logger.info("Starting session...")
    val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .appName("Dataset Splitter")
      .getOrCreate()
    implicit val settings = Settings(params, spark)
    import spark.implicits._
    logger.info("Starting session... Done!")

    // Reading polygons...
    val (polygonsA, nPolygonsA) = readPolygonsA
    val (polygonsB, nPolygonsB) = readPolygonsB

    logger.info(s"Polygons in A|$nPolygonsA")
    logger.info(s"Polygons in B|$nPolygonsB")
    save{"/tmp/edgesA.wkt"}{
      polygonsA.map{ a =>
        s"${a.toText}\t${a.getUserData}\n"
      }.collect()
    }
    save{"/tmp/edgesB.wkt"}{
      polygonsB.map{ b =>
        s"${b.toText}\t${b.getUserData}\n"
      }.collect()
    }

    // Partitioning edges...
    val A = new SpatialRDD[Polygon]()
    A.setRawSpatialRDD(polygonsA)
    A.analyze()
    val boundaryA = envelope2Polygon(A.boundaryEnvelope)
    
    val B = new SpatialRDD[Polygon]()
    B.setRawSpatialRDD(polygonsB)
    B.analyze()
    val boundaryB = envelope2Polygon(B.boundaryEnvelope)

    val boundary = new QuadRectangle(boundaryA.union(boundaryB).getEnvelopeInternal)
    val polygons = if(nPolygonsA > nPolygonsB) polygonsA else polygonsB
    val quadtree = getQuadTree(polygons, boundary)

    val partitioner = new QuadTreePartitioner(quadtree)
    A.spatialPartitioning(partitioner)
    A.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
    B.spatialPartitioning(partitioner)
    B.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
      
    val grids = spark.sparkContext.broadcast{
      quadtree.getLeafZones.asScala
        .map{ grid => (grid.partitionId.toInt, grid) }
        .sortBy(_._1)
        .map{ case (i,q) =>
          val poly = envelope2Polygon(q.getEnvelope)
          poly.setUserData(i)
          poly
        }
    }

    logger.info(f"Total number of partitions|${grids.value.size}")
    logger.info(f"Total number of polygons in A|${A.spatialPartitionedRDD.count()}")
    logger.info(f"Total number of polygons in B|${B.spatialPartitionedRDD.count()}")
    save{"/tmp/edgesGrids.wkt"}{
      grids.value.map{ g =>
        s"${g.toText()}\t${g.getUserData().toString}\n"
      }
    }

    save{"/tmp/edgesAsort.wkt"}{
      A.spatialPartitionedRDD.rdd.mapPartitionsWithIndex{ case(index, polygons)=>
        polygons.map{ polygon =>
          (polygon, index)
        }.toIterator
      }.groupBy(_._1).values.map{ pairs =>
        val sortByArea = pairs.toVector.map{ case(polygon, index) =>
          val grid = grids.value(index)
          val area = polygon.intersection(grid).getArea
          (area, (polygon, index))
        }.sortBy(_._1)(Ordering[Double].reverse)

        sortByArea.head
      }.map(_._2)
      .map{ case(p, i) => s"${p.toText()}\t${i}\n"}.collect()
    }
    save{"/tmp/edgesBsort.wkt"}{
      B.spatialPartitionedRDD.rdd.mapPartitionsWithIndex{ case(index, polygons)=>
        polygons.map{ polygon =>
          (polygon, index)
        }.toIterator
      }.groupBy(_._1).values.map{ pairs =>
        val sortByArea = pairs.toVector.map{ case(polygon, index) =>
          val grid = grids.value(index)
          val area = polygon.intersection(grid).getArea
          (area, (polygon, index))
        }.sortBy(_._1)(Ordering[Double].reverse)

        sortByArea.head
      }.map(_._2)
      .map{ case(p, i) => s"${p.toText()}\t${i}\n"}.collect()
    }

    val perc = params.perc()
    val nGrids = grids.value.size
    val step = (nGrids / perc).toInt


  }
}

class SplitterConf(args: Seq[String]) extends ScallopConf(args) {
  val input1:      ScallopOption[String]  = opt[String]  (required = true)
  val offset1:     ScallopOption[Int]     = opt[Int]     (default = Some(0))
  val input2:      ScallopOption[String]  = opt[String]  (required = true)
  val offset2:     ScallopOption[Int]     = opt[Int]     (default = Some(0))
  val grid:        ScallopOption[String]  = opt[String]  (default = Some("QUADTREE"))
  val partitions:  ScallopOption[Int]     = opt[Int]     (default = Some(512))
  val fraction:    ScallopOption[Double]  = opt[Double]  (default = Some(0.25))
  val capacity:    ScallopOption[Int]     = opt[Int]     (default = Some(100))
  val levels:      ScallopOption[Int]     = opt[Int]     (default = Some(6))
  val perc:        ScallopOption[Int]     = opt[Int]     (default = Some(10))
  val debug:       ScallopOption[Boolean] = opt[Boolean] (default = Some(false))

  verify()
}

