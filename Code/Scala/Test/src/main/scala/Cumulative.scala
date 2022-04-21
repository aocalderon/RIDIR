package edu.ucr.dblab

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.rdd.RDD

import org.slf4j.{LoggerFactory, Logger}
import org.rogach.scallop._

import scala.annotation.tailrec
import java.nio.file.Paths

object Cumulative {
  def main(args: Array[String]): Unit = {
    implicit val logger: Logger = LoggerFactory.getLogger("myLogger")
    logger.info("Starting session...")
    val params = new CumulativeConf(args)
    implicit val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .appName("Cumulative")
      .getOrCreate()
    import spark.implicits._
    val appId = spark.sparkContext.getConf.get("spark.app.id")

    logger.info("Reading data...")
    val data = spark.read.text(params.input()).rdd.map{ line =>
      val arr = line.getString(0).split("\t")
      val wkt   = arr(0)
      val edges = arr(3).toInt
      val cumulative = arr(4).toLong

      (wkt, edges, cumulative)
    }.cache()

    logger.info("Doing the splits...")    
    val total_edges = data.map{ case(w, e, c) => e }.reduce(_ + _)
    val parts = params.parts()
    val bound = total_edges * 1.0 / parts 
    val bounds = (1 to parts).map{_ * bound}.toList
    val rdds = splitByBound(bounds, data, List.empty[RDD[String]])

    logger.info("Saving data...")
    val filename = Paths.get(params.input()).getFileName.toString
    val tag = filename.split("\\.")(0)
    rdds.zipWithIndex.foreach{ case(rdd, id) =>
      rdd.toDF.write.mode(SaveMode.Overwrite).text(s"${params.output()}/${tag}_${id}")
    }

    logger.info("Closing session...")
    spark.close
  }

  @tailrec
  def splitByBound(bounds: List[Double], data: RDD[(String, Int, Long)],
    splits: List[RDD[String]]): List[RDD[String]] = {

    bounds match {
      case Nil => splits
      case head :: tail => {
        val sample = data.filter{ case(wkt, edges, cumulative) => cumulative <= head }
          .map{ case(wkt, edges, cumulative) => wkt }
        splitByBound(tail, data, splits :+ sample)
      }
    }
    
  }
}

class CumulativeConf(args: Seq[String]) extends ScallopConf(args) {
  val input:  ScallopOption[String]  = opt[String] (required = true)
  val output: ScallopOption[String]  = opt[String] (required = true)
  val parts:  ScallopOption[Int]     = opt[Int]    (default  = Some(4))
  val partitions: ScallopOption[Int] = opt[Int]    (default  = Some(960))

  verify()
}
