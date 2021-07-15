package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.geom.{Geometry, Polygon}
import com.vividsolutions.jts.io.WKTReader

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

import edu.ucr.dblab.sdcel.geometries.Half_edge

import Utils._

object DCELOverlay2 {
  case class FaceViz(boundary: Polygon, tag: String)

  def overlapOp(faces: RDD[FaceViz], op: (Iterator[FaceViz]) => Iterator[List[(String, Geometry)]], output_name: String)
    (implicit geofactory: GeometryFactory, settings: Settings, spark: SparkSession){

    val results = faces.mapPartitions{op}
      .flatMap{ _.map{ case(tag, geom) => (tag, geom.toText()) } }
      .reduceByKey{ case(a, b) => mergePolygons(a, b) }
    results.cache()

    if(settings.local){
      val r = results.map{case(id, wkt) => s"$wkt\t$id\n" }.collect
      save{s"${output_name}.wkt"}{ r }
      logger.info(s"Saved operation at ${output_name}.wkt [${r.size} results].")
    } else {
      import spark.implicits._
      results.map{ case(id, wkt) => s"$wkt\t$id" }.toDS
        .write.mode("overwrite").text(output_name)
      logger.info(s"Saved operation at ${output_name} [${results.count()} results].")
    }
  }

  def intersection(faces: Iterator[FaceViz]): Iterator[List[(String, Geometry)]] = {
    val ldcel = faces.filter{ _.tag.split(" ").size == 2 }
      .map{ face => (face.tag, face.boundary) }
      .toList
    Iterator(ldcel)
  }
  def difference(faces: Iterator[FaceViz]): Iterator[List[(String, Geometry)]] = {
    val ldcel = faces.filter{ _.tag.split(" ").size == 1 }
      .map{ face => (face.tag, face.boundary) }
      .toList
    Iterator(ldcel)
  }
  def union(faces: Iterator[FaceViz]): Iterator[List[(String, Geometry)]] = {
    val ldcel = faces
      .map{ face => (face.tag, face.boundary) }
      .toList
    Iterator(ldcel)
  }
  def differenceA(faces: Iterator[FaceViz]): Iterator[List[(String, Geometry)]] = {
    difference(faces).map{ _.filter{_._1.substring(0, 1) == "A"} }
  }
  def differenceB(faces: Iterator[FaceViz]): Iterator[List[(String, Geometry)]] = {
    difference(faces).map{ _.filter{_._1.substring(0, 1) == "B"} }
  }

  def mergePolygons(a: String, b:String)
    (implicit geofactory: GeometryFactory): String = {
    val reader = new WKTReader(geofactory)
    val pa = reader.read(a)
    val pb = reader.read(b)
    if(pa.getArea < 0.001 || pa.getArea.isNaN()) b
    else if(pb.getArea < 0.001 || pb.getArea.isNaN()) a
    else{
      geofactory.createGeometryCollection(Array(pa, pb)).buffer(0.0).toText()
    }
  }
}
