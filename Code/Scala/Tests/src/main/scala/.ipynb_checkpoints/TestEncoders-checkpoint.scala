package edu.ucr.dblab.tests

import org.apache.spark.sql.SparkSession
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{Dataset, Encoder, Encoders}
import org.apache.spark.sql.functions._

import org.apache.sedona.sql.utils.{Adapter, SedonaSQLRegistrator}
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.core.spatialRDD.SpatialRDD

import org.locationtech.jts.geom.{PrecisionModel, GeometryFactory}
import org.locationtech.jts.geom.{Geometry, Envelope, Coordinate}
import org.locationtech.jts.geom.Point

import scala.util.Random

import edu.ucr.dblab.tests.GeoEncoders._

object TestEncoders {
  def main(args: Array[String]) = {
    val n = 10000
    implicit val spark:SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SedonaTest")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)
      .getOrCreate()
    SedonaSQLRegistrator.registerAll(spark)
    import spark.implicits._

    implicit val model = new PrecisionModel(1000)
    implicit val geofactory = new GeometryFactory(model)

    val spDS = spark.createDataset{ (0 to n).map{ x =>
      geofactory.createPoint(new Coordinate(x,x))}
    }
    show(spDS)

    spDS.map{_.buffer(5.0)}.map(_.toText).show

    val r = Random
    val data = for{
      i <- (0 to n)
    } yield {
      val x = r.nextDouble
      val y = r.nextDouble

      geofactory.createPoint(new Coordinate(x, y))
    }

    val rdd = spark.sparkContext.parallelize(data)
    val spRDD = new SpatialRDD[Point]
    spRDD.setRawSpatialRDD(rdd)
    spRDD.analyze
    val spDF = Adapter.toDf(spRDD, spark)
    spDF.show

    val epsilon = 5.0
    val j = spDF.as("a")
      .join(spDF.as("b"), expr(s"ST_Distance(a.geometry, b.geometry) < $epsilon"))

    val envelopes = spDF.map{ x =>
      val p = x.getAs[Point](0)
      val e = p.getEnvelopeInternal
      e.expandBy(10.0)
      e
    }

    show(envelopes)

    spark.close
  }

  def show[T](ds: Dataset[T])(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    ds.map{_.toString}.show
  }
}
