package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.geom.{Geometry, Polygon}
import com.vividsolutions.jts.io.WKTReader

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

import scala.annotation.tailrec

import edu.ucr.dblab.sdcel.geometries.Half_edge

import Utils._

object DCELOverlay2 {
  def overlay2(sdcel: RDD[(Half_edge, String)]): RDD[(Half_edge, String)] = {
    sdcel.mapPartitionsWithIndex{ (pid, it) =>
      it.flatMap{ case(hedge, label) =>
        if(hedge.isValid){
          getValidSegments(hedge, hedge.prev, List(hedge)).map(h => (h, label))
        } else {
          val valid = getNextValidOrStop(hedge.next, hedge, hedge)
          if(valid == hedge){
            List.empty[(Half_edge, String)]
          } else {
            getValidSegments(valid, valid.prev, List(valid)).map(h => (h, label))
          }
        }
      }
    }
  }

  def overlay3(it: List[(Half_edge, String)]): List[(Half_edge, String)] = {
    it.flatMap{ case(hedge, label) =>
      if(hedge.isValid){
        getValidSegments(hedge, hedge.prev, List(hedge)).map(h => (h, label))
      } else {
        val valid = getNextValidOrStop(hedge.next, hedge, hedge)
        if(valid == hedge){
          List.empty[(Half_edge, String)]
        } else {
          getValidSegments(valid, valid.prev, List(valid)).map(h => (h, label))
        }
      }
    }
  }

  def getValidSegments(hcurr: Half_edge, hstop: Half_edge, list: List[Half_edge])
      : List[Half_edge] = {

    if(hcurr == hstop){
      hcurr.next = null
      list
    } else {
      //println(hcurr)
      if(hcurr.next.isValid){
        getValidSegments(hcurr.next, hstop, list)
      } else {
        val valid = getNextValidOrStop(hcurr.next, hcurr, hstop)
        hcurr.next = null
        if(valid == hstop){
          valid.next = null
          if(valid.isValid) list :+ valid else list
        } else {
          getValidSegments(valid, hstop, list :+ valid)
        }
      }
    }
  }

  def overlay4(it: List[(Half_edge, String)]): List[(Half_edge, Half_edge, String)] = {
    it.flatMap{ case(hedge, label) =>
      if(hedge.isValid){
        val e = getValidSegments2(hedge, hedge.prev, List(hedge))
        println("Test1")
        e.foreach(println)
        val (e1, e2) = e.zipWithIndex.partition(_._2 % 2 == 0)
        e1.map(_._1).zip(e2.map(_._1)).map{ case(s,e) => (s, e, label)}
      } else {
        val valid = getNextValidOrStop(hedge.next, hedge, hedge)
        if(valid == hedge){
          List.empty[(Half_edge, Half_edge, String)]
        } else {
          val e = getValidSegments2(valid, valid.prev, List(valid))
          println("Test2")
          e.foreach(println)
          val (e1, e2) = e.zipWithIndex.partition(_._2 % 2 == 0)
          e1.map(_._1).zip(e2.map(_._1)).map{ case(s,e) => (s, e, label)}
        }
      }
    }
  }

  def getValidSegments2(hcurr: Half_edge, hstop: Half_edge, list: List[Half_edge])
      : List[Half_edge] = {

    if(hcurr == hstop){
      hcurr.next = null
      if(hcurr.isValid) list :+ hcurr else list :+ hcurr.prev
    } else {
      println(hcurr)
      if(hcurr.next.isValid){
        getValidSegments2(hcurr.next, hstop, list)
      } else {
        val valid = getNextValidOrStop(hcurr.next, hcurr, hstop)
        hcurr.next = null
        if(valid == hstop){
          valid.next = null
          if(valid.isValid) list ++ List(hcurr, valid, valid) else list :+ hcurr
        } else {
          getValidSegments2(valid, hstop, list ++ List(hcurr,valid))
        }
      }
    }
  }

  @tailrec
  def getNextValidOrStop(curr: Half_edge, stop1: Half_edge, stop2: Half_edge): Half_edge = {
    if(curr == stop2){
      stop2 // mark than we do a complete cycle early...
    }else if(curr == stop1){
      stop1 // all the nexts are invalid...
    } else if(curr.isValid){
      curr
    } else {
      getNextValidOrStop(curr.next, stop1, stop2)
    }
  }

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
