package edu.ucr.dblab.sdcel.reader

import edu.ucr.dblab.sdcel.Params
import edu.ucr.dblab.sdcel.Utils.{Settings, log}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

object IDAdder {
  def main(args: Array[String]): Unit = {
    // Starting session...
    implicit val params: Params = new Params(args)
    implicit val spark: SparkSession = SparkSession.builder()
      .master(params.master())
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .getOrCreate()
    import spark.implicits._

    implicit val S: Settings = Settings(
      debug = params.debug(),
      appId = spark.sparkContext.applicationId
    )
    val command = System.getProperty("sun.java.command")
    log(s"COMMAND|$command")
    log("TIME|Start")

    val dataset = spark.read.textFile(params.input1()).rdd.map{ line =>
      val arr = line.split("\t")
      arr(0)
    }.zipWithUniqueId().map{ case(wkt, id) =>
      s"$wkt\t$id"
    }


    dataset.toDF.write
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .text(params.output())

    log("TIME|End")
    spark.close
  }
}
