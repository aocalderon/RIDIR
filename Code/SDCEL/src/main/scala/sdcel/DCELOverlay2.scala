package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.geom.{Geometry, Polygon, LineString, Coordinate}
import com.vividsolutions.jts.io.WKTReader

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

import scala.annotation.tailrec

import edu.ucr.dblab.sdcel.geometries.{Half_edge, Segment, Coords}

import Utils._


object DCELOverlay2 {

  def mergeSegs(sdcel: RDD[(Segment, String)])
      (implicit geofactory: GeometryFactory, settings: Settings): RDD[(String, Polygon)] = {

    val a = sdcel.filter{!_._1.isClose}.map{ case(s,l) => (l,List(s.line)) }
      .reduceByKey{ case(a, b) => a ++ b }
    a//.flatMap{ case(l, ss) => ss.map{s => (s,l)}}
    .mapPartitions{ it =>
        val reader = new WKTReader(geofactory)
        it.flatMap{ case(l,ss) =>
          val lines = ss.map(s => reader.read(s).asInstanceOf[LineString])
          //lines
          val ps = mergeLines(lines)
          ps.map{ p => (l,p) }
        }
      }

  }

  def mergeLines(lines: List[LineString])
    (implicit geofactory: GeometryFactory, settings: Settings): List[Polygon] = {
    val coords = lines.map(l => Coords(l.getCoordinates)).toSet
    val C = mergeCoordinates(coords.tail, coords.head, List.empty[Coords])
    C.filter(_.getCoords.size >= 4).map{c =>
      geofactory.createPolygon(c.getCoords)
    }
  }

  @tailrec
  def mergeCoordinates(coords: Set[Coords], curr: Coords, r: List[Coords])
      (implicit geofactory: GeometryFactory): List[Coords] = {
    if(coords.isEmpty){
      r :+ curr
    } else if(curr.isClose){
      val n_r = r :+ curr
      val n_curr = coords.head
      val n_coords = coords.tail
      mergeCoordinates(n_coords, n_curr, n_r)
    } else {
      val next = try {
        coords.filter(c => curr.touch(c.first)).head
      } catch {
        case e: java.util.NoSuchElementException => {
          println("Coords")
          coords.map{C => geofactory.createLineString(C.coords).toText}.foreach(println)
          println("Current")
          println(geofactory.createLineString(curr.coords).toText)
          System.exit(1)
          curr
        }
      }
      val n_coords = coords -- Set(next)
      val n_curr = Coords(curr.coords ++ next.coords)
      mergeCoordinates(n_coords, n_curr, r)
    }
  }

  def overlay4(sdcel: RDD[(Half_edge, String)]): RDD[(Segment, String)] = {
    sdcel.mapPartitionsWithIndex{ (pid, it) =>
      val partitionId = -1
      val labelId = ""
      it.flatMap{ case(hedge, label) =>
        if(hedge.isValid){
          val s = getValidSegments2(hedge, hedge.prev, List(hedge), List.empty[Segment],
            -1)
          if(pid == partitionId){
            if(label == labelId){
              println(s"$labelId is valid? ${hedge.isValid}")
              println(hedge)
              println(hedge.prev)
              println("Seg")
              s.foreach(println)
            }
          }
          
          s.map(h => (h, label))
        } else {
          val valid = getNextValidOrStop(hedge.next, hedge, hedge)
          if(valid == hedge){
            List.empty[(Segment, String)]
          } else {
            getValidSegments2(valid, valid.prev, List(valid), List.empty[Segment])
              .map(h => (h, label))
          }
        }
      }
    }
  }

  def overlay2(sdcel: RDD[(Half_edge, String)]): RDD[(Half_edge, String)] = {
    sdcel.mapPartitionsWithIndex{ (pid, it) =>
      it.flatMap{ case(hedge, label) =>
        val segs = if(hedge.isValid){
          getValidSegments(hedge, hedge.prev, List(hedge)).map(h => (h, label))
        } else {
          val valid = getNextValidOrStop(hedge.next, hedge, hedge)
          if(valid == hedge){
            List.empty[(Half_edge, String)]
          } else {
            getValidSegments(valid, valid.prev, List(valid)).map(h => (h, label))
          }
        }
        segs
      }
    }
  }
 
  def overlay3(it: List[(Half_edge, String)]): List[(Segment, String)] = {
    it.flatMap{ case(hedge, label) =>
      if(hedge.isValid){
        println("Segments")
        getValidSegments2(hedge, hedge.prev, List(hedge), List.empty[Segment])
          .map(h => (h, label))
      } else {
        val valid = getNextValidOrStop(hedge.next, hedge, hedge)
        if(valid == hedge){
          List.empty[(Segment, String)]
        } else {
          getValidSegments2(valid, valid.prev, List(valid), List.empty[Segment])
            .map(h => (h, label))
        }
      }
    }
  }

  @tailrec
  def getValidSegments2(hcurr: Half_edge, hstop: Half_edge, listH: List[Half_edge],
    listS: List[Segment], p: Int = -1, l: String = ""): List[Segment] = {

    val pid = org.apache.spark.TaskContext.getPartitionId
    if(pid == p && l.contains(hcurr.label)){
      println(hcurr)
    }

    if(hcurr == hstop){
      hcurr.next = null
      val s = Segment(listH)
      listS :+ s
    } else {
      if(hcurr.next.isValid){
        val next = hcurr.next
        getValidSegments2(next, hstop, listH :+ next, listS,p)
      } else {
        val valid = getNextValidOrStop(hcurr.next, hcurr, hstop)
        hcurr.next = null
        if(valid == hstop){
          valid.next = null
          if(valid.isValid){
            val list = listH :+ hcurr
            val s1 = Segment(list)
            val s2 = Segment(List(valid, valid))
            listS ++ List(s1,s2)
          } else {
            val s = Segment(listH)
            listS :+ s
          }
        } else {
          val s = Segment(listH)
          getValidSegments2(valid, hstop, List(valid), listS :+ s,p)
        }
      }
    }
  }

  @tailrec
  def getValidSegments(hcurr: Half_edge, hstop: Half_edge, list: List[Half_edge])
      : List[Half_edge] = {

    if(hcurr == hstop){
      hcurr.next = null
      list
    } else {
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
