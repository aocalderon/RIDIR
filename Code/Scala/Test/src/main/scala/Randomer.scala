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
    val input = "file:///home/acald013/Datasets/Test/T1.wkt"
    val polys = spark.read.text(input).rdd.zipWithIndex.map{ case(line, id) =>
      val wkt = line.getString(0).split("\t")(0)
      s"$wkt\t$id\n"
    }.toDF().orderBy(rand()).map{_.getString(0)}

    polys.toDF().show

    val f = new FileWriter("/tmp/A_prime.wkt")
    f.write(polys.collect.mkString(""))
    f.close

    spark.close
  }
}
