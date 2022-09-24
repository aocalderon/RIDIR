package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geom.{ GeometryFactory, PrecisionModel }
import com.vividsolutions.jts.geom.{ Geometry, Envelope, Coordinate }
import com.vividsolutions.jts.geom.{ Polygon, LineString, Point }
import com.vividsolutions.jts.io.WKTReader

import org.apache.spark.sql.{ SparkSession, Dataset, Row, functions, SaveMode }
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{ TaskContext, SparkEnv }

import ch.cern.sparkmeasure.TaskMetrics
import org.slf4j.{ Logger, LoggerFactory }

import edu.ucr.dblab.sdcel.geometries.{ Half_edge, Cell, EdgeData }
import edu.ucr.dblab.sdcel.cells.EmptyCellManager2.{ getFaces, EmptyCell }
import DCELMerger2.{groupByNextMBRPoly}

object Utils {
  //** Implicits
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  //** Case Class
  case class Timestamp(t: Long)
  case class Tick(var t: Long)
  case class Settings(
    tolerance: Double = 1e-3,
    debug: Boolean = false,
    local: Boolean = false,
    seed: Long = 42L,
    appId: String = "0",
    persistance: StorageLevel = StorageLevel.MEMORY_ONLY_2,
    ooption: Int = 0,
    olevel: Int = 4
  ){
    val scale = 1 / tolerance
  }

  //** Functions
  def envelope2WKT(envelope: Envelope)(implicit geofactory: GeometryFactory): String = {
    geofactory.toGeometry(envelope).toText
  }
  def log(msg: String)(implicit settings: Settings): Unit = {
    val now = System.currentTimeMillis
    logger.info(s"${settings.appId}|$msg")
  }

  def log2(msg: String)(implicit prev: Tick, settings: Settings): Unit = {
    val now = System.currentTimeMillis
    val duration = now - prev.t
    
    logger.info(s"${settings.appId}|$duration|$msg")
    prev.t = now
  }

  def getPartitionLocation(pid: Long)(implicit settings: Settings): String = {
    val eid  = SparkEnv.get.executorId
    val host = java.net.InetAddress.getLocalHost().getHostName()
    logger.info(s"Partition $pid at executor $eid in $host...")
    s"${settings.appId}|${host}:${eid}"
  }

  def saveSDCEL(output: String, 
    sdcel: RDD[(Half_edge, String, Envelope, Polygon)], m: Map[String, EmptyCell])
    (implicit geofactory: GeometryFactory, cells: Map[Int, Cell], settings: Settings,
      spark: SparkSession) = {

    import spark.implicits._
    case class Face(h: Half_edge, l: String, e: Envelope, p: Polygon, pid: Int){
      val id = h.id
      override def toString: String = s"$p\t$pid\t$id\t$l"
    }

    sdcel.mapPartitionsWithIndex{ (pid, it) =>
      val faces = getFaces(it, cells(pid), m).map{ case(h,l,e,p) => Face(h,l,e,p,pid) }
      val hedges = faces.flatMap{_.h.getNexts}

      hedges.map{ h => Half_edge.save(h, pid) }.toIterator
    }.toDS.write.mode(SaveMode.Overwrite).text(output + "/hedges")
    log(s"INFO|SDCEL half edges saved at ${output}/hedges")

    sdcel.mapPartitionsWithIndex{ (pid, it) =>
      val faces = getFaces(it, cells(pid), m)
        .map{ case(h,l,e,p) => Face(h,l,e,p,pid).toString }

      faces.toIterator
    }.toDS.write.mode(SaveMode.Overwrite).text(output + "/faces")

    log(s"INFO|SDCEL faces saved at ${output}/faces")
  }

  def loadSDCEL(input: String, letter: String = "A")
    (implicit spark: SparkSession, geofactory: GeometryFactory, cells: Map[Int, Cell],
      settings: Settings): RDD[(Half_edge, String, Envelope, Polygon)] = {

    val input_base = input.split("/edges")(0)
    val partitions = cells.size
    val sdcel = spark.read.textFile(s"${input_base}/ldcel${letter}/hedges").rdd
      .mapPartitionsWithIndex{ case(pid, lines) =>
        val reader = new WKTReader(geofactory)
        lines.map{ line =>
          val hedge = Half_edge.load(line)
          (hedge.partitionId, hedge)
        }.toIterator
      }
      .partitionBy(new SimplePartitioner(partitions))
      .mapPartitionsWithIndex{ (pid, it) =>
        val hs = it.toList
        val hedgesMap = hs.map{ case(pid, edge) =>  edge.id -> edge }.toMap
        val hedges = hs.map{ case(pid, edge) =>
          val pointers = edge.pointers
          edge.prev = try{ hedgesMap(pointers(0)) } catch {
            case e: java.util.NoSuchElementException => {
              logger.info(s"NULL at prev. $pid ${edge.wkt} ${edge.pointers.mkString(" ")}")
              null
            }
          }
          edge.twin = try{ hedgesMap(pointers(1)) } catch {
            case e: java.util.NoSuchElementException => {
              logger.info(s"NULL at twin. $pid ${edge.wkt} ${edge.pointers.mkString(" ")}")
              edge.reverse
            }
          }
          edge.next = try{ hedgesMap(pointers(2)) } catch {
            case e: java.util.NoSuchElementException => {
              logger.info(s"NULL at next. $pid ${edge.wkt} ${edge.pointers.mkString(" ")}")
              null
            }
          }

          edge
        }

        groupByNextMBRPoly((hedges).toSet, List.empty[(Half_edge, String, Envelope, Polygon)])
          .filter(_._2.split(" ").size == 1).toIterator
      }.persist(settings.persistance)

    sdcel
  }

  def save(filename: String)(content: Seq[String]): Unit = {
    val start = clocktime
    val f = new java.io.PrintWriter(filename)
    f.write(content.mkString(""))
    f.close
    val end = clocktime
    val time = "%.2f".format((end - start) / 1000.0)
    logger.info(s"Saved ${filename} in ${time}s [${content.size} records].")
  }

  def clocktime = System.currentTimeMillis()

  def getPhaseMetrics(metrics: TaskMetrics, phaseName: String)(implicit settings: Settings): Dataset[Row] = {
    metrics.createTaskMetricsDF()
      .withColumn("appId", functions.lit(settings.appId))
      .withColumn("phaseName", functions.lit(phaseName))
      .select("host", "index", "launchTime", "finishTime", "duration", "appId", "phaseName")
      .orderBy("launchTime")
  }

  def round(number: Double)(implicit geofactory: GeometryFactory): Double = {
    val scale = geofactory.getPrecisionModel.getScale
    Math.round(number * scale) / scale
  }

  def round(number: Double, decimals: Int): Double = {
    val scale = Math.pow(10, decimals)
    Math.round(number * scale) / scale
  }

  import Numeric.Implicits._
  def mean[T: Numeric](xs: Iterable[T]): Double = xs.sum.toDouble / xs.size
  def variance[T: Numeric](xs: Iterable[T]): Double = {
    val avg = mean(xs)
    xs.map(_.toDouble).map(a => math.pow(a - avg, 2)).sum / xs.size
  }
  def stdDev[T: Numeric](xs: Iterable[T]): Double = math.sqrt(variance(xs))
}

