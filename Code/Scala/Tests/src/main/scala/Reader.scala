package edu.ucr.dblab.tests

import org.apache.spark.sql.SparkSession
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.sedona.core.enums.GridType
import org.locationtech.jts.geom.{PrecisionModel, GeometryFactory}
import org.locationtech.jts.geom.{Geometry, Envelope, Coordinate}
import org.locationtech.jts.geom.{Polygon, LineString, Point}
import org.locationtech.jts.io.WKTReader
import scala.util.Random
import java.io.PrintWriter
import collection.JavaConverters._

object Reader {
  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession.builder().getOrCreate
    import spark.implicits._
    implicit val model = new PrecisionModel(1000)
    implicit val geofactory = new GeometryFactory(model)

    val points = (0 to 10800).map{ i =>
      geofactory.createPoint(new Coordinate(Random.nextDouble(), Random.nextDouble()))
    }

    val pointsRDD_prime = spark.sparkContext.parallelize(points, 108)
    val pointsRDD = new SpatialRDD[Point]
    pointsRDD.setRawSpatialRDD(pointsRDD_prime)
    pointsRDD.analyze()
    pointsRDD.spatialPartitioning(GridType.QUADTREE, 256)

    val p = new PrintWriter("/tmp/edgesP.wkt")
    val pWKT = pointsRDD.spatialPartitionedRDD.rdd.mapPartitionsWithIndex{ (pid, points) =>
      points.map{ point =>
        val wkt = point.toText()
        s"$wkt\t$pid\n"
      }
    }.collect
    p.write(pWKT.mkString(""))
    p.close

    val grids = pointsRDD.getPartitioner.getGrids

    val g = new PrintWriter("/tmp/edgesG.wkt")
    val gWKT = grids.asScala.zipWithIndex.map{ case(grid, id) =>
      val wkt = geofactory.toGeometry(grid).toText
      s"$wkt\t$id\n"
    }
    g.write(gWKT.mkString(""))
    g.close

    spark.close()
  }
}
