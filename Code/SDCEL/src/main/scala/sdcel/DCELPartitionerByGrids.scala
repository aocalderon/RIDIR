package sdcel

import com.vividsolutions.jts.geom._
import com.vividsolutions.jts.index.strtree.STRtree
import com.vividsolutions.jts.io.WKTReader
import edu.ucr.dblab.sdcel.DCELPartitioner2.{getEdgesWithCrossingInfo, read, read2}
import edu.ucr.dblab.sdcel.Utils.{Settings, log, save}
import edu.ucr.dblab.sdcel.geometries.{Cell, EdgeData}
import edu.ucr.dblab.sdcel.{Params, SimplePartitioner}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.io.Source

object DCELPartitionerByGrids {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")
  def readGrids(grids: String)(implicit G: GeometryFactory): Map[Int, Cell] = {
    val reader = new WKTReader(G)
    val buffer = Source.fromFile(grids)
    val cells = buffer.getLines().map{ line =>
      val arr = line.split("\t")
      val poly = reader.read(arr(0)).asInstanceOf[Polygon]
      val mbr = G.createLinearRing(poly.getCoordinates)
      val cid = arr(1).toInt
      val cell = Cell(cid, "", mbr)
      cid -> cell
    }.toMap
    buffer.close
    cells
  }
  def main(args: Array[String]) = {
    // Starting session...
    implicit val params: Params = new Params(args)
    implicit val spark: SparkSession = SparkSession.builder()
      .master(params.master())
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()
    import spark.implicits._
    implicit val settings = Settings(
      tolerance = params.tolerance(),
      debug = params.debug(),
      local = params.local(),
      appId = spark.sparkContext.applicationId
    )
    val command = System.getProperty("sun.java.command")
    log(s"COMMAND|$command")
    log(s"INFO|scale=${settings.scale}")
    val model = new PrecisionModel(settings.scale)
    implicit val geofactory = new GeometryFactory(model)
    log("TIMEP|Start")

    // Reading data...
    val edgesRDDA = if(!params.readid()) read(params.input1()) else read2(params.input1())
    val nEdgesRDDA = edgesRDDA.getRawSpatialRDD.count()
    log(s"INFO|edgesA=$nEdgesRDDA")
    val edgesRDDB = if(!params.readid()) read(params.input2()) else read2(params.input2())
    val nEdgesRDDB = edgesRDDB.getRawSpatialRDD.count()
    log(s"INFO|edgesB=$nEdgesRDDB")

    //val quadtree = new StandardQuadTree[LineString]()

    // Reading cells from file...
    val grids = readGrids(params.grids())
    val tree = new STRtree()
    grids.foreach{ case(id, cell) =>
      val env = cell.mbr.getEnvelopeInternal
      tree.insert(env, id)
    }
    val edgesA = edgesRDDA.getRawSpatialRDD.rdd.mapPartitions{ edges =>
      edges.flatMap{ edge =>
        val envelope = edge.getEnvelopeInternal
        tree.query(envelope).asScala.map{ pid =>
          (pid.asInstanceOf[Int], edge)
        }
      }
    }.partitionBy(new SimplePartitioner[LineString](grids.size)).map(_._2).cache
    val edgesB = edgesRDDB.getRawSpatialRDD.rdd.mapPartitions{ edges =>
      edges.flatMap{ edge =>
        val envelope = edge.getEnvelopeInternal
        tree.query(envelope).asScala.map{ pid =>
          (pid.asInstanceOf[Int], edge)
        }
      }
    }.partitionBy(new SimplePartitioner[LineString](grids.size)).map(_._2).cache


    // Saving to HDFS adding info about edges crossing border cells...
    //saveToHDFSWithCrossingInfo(edgesA, cells, params.apath())
    //saveToHDFSWithCrossingInfo(edgesB, cells, params.bpath())
    //log("TIMEP|Saving")

    val A = getEdgesWithCrossingInfo(edgesA, grids, "A")
    val B = getEdgesWithCrossingInfo(edgesB, grids, "B")

    save("/tmp/edgesACross.wkt") {
      A.map{ edge =>
        val wkt = edge.toText
        val dat = edge.getUserData.asInstanceOf[EdgeData]

        s"$wkt\t$dat\n"
      }.collect
    }
    save("/tmp/edgesBCross.wkt") {
      B.map{ edge =>
        val wkt = edge.toText
        val dat = edge.getUserData.asInstanceOf[EdgeData]

        s"$wkt\t$dat\n"
      }.collect
    }

    spark.close
  }

}
