package edu.ucr.dblab

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.rand
import java.io.FileWriter

object Randomer {
  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession.builder()
      .appName("Randomer")
      .getOrCreate()

    import spark.implicits._
    val params = new RandomerConf(args)
    val input = params.input()
    val polys = spark.read.text(input).rdd.zipWithIndex.map{ case(line, id) =>
      val wkt = line.getString(0).split("\t")(0)
      s"$wkt\t$id\n"
    }.toDF().orderBy(rand()).map{_.getString(0)}

    polys.toDF().show

    val output = params.output()
    val f = new FileWriter("/tmp/B_prime.wkt")
    f.write(polys.collect.mkString(""))
    f.close

    spark.close
  }
}

import org.rogach.scallop._

class RandomerConf(args: Seq[String]) extends ScallopConf(args) {
  val input: ScallopOption[String]  = opt[String]  (required = true)
  val output: ScallopOption[String]  = opt[String]  (required = true)

  verify()
}
