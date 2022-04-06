package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geom.{ PrecisionModel, GeometryFactory }
import com.vividsolutions.jts.geom.{ Geometry, Coordinate, Envelope }
import com.vividsolutions.jts.geom.{ Polygon, LineString }
import com.vividsolutions.jts.io.WKTReader

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.TaskContext

import scala.annotation.tailrec

import edu.ucr.dblab.sdcel.geometries.{ Half_edge, Cell, Segment, Coords, Coord }
import edu.ucr.dblab.sdcel.cells.EmptyCellManager2.{ EmptyCell, getFaces }
import edu.ucr.dblab.sdcel.DCELMerger2.merge
import edu.ucr.dblab.sdcel.Utils.{ Settings, logger, save }
import edu.ucr.dblab.sdcel.CellAgg.{ aggregateSegments, aggregateHedges }

object DCELOverlay2 {
  def overlay(
    ldcelA: RDD[(Half_edge, String, Envelope, Polygon)], ma: Map[String, EmptyCell],
    ldcelB: RDD[(Half_edge, String, Envelope, Polygon)], mb: Map[String, EmptyCell])
    (implicit cells: Map[Int, Cell], geofactory: GeometryFactory, settings: Settings)
      : RDD[(String, Polygon)] = {

    val sdcel_prime = ldcelA.zipPartitions(ldcelB,
      preservesPartitioning=true){ (iterA, iterB) =>

      val pid = TaskContext.getPartitionId
      val cell = cells(pid)

      val A = getFaces(iterA, cell, ma)
      val B = getFaces(iterB, cell, mb)

      val hedges = merge(A, B, cells)
      hedges.toIterator
    }

    mergeSegments{ collectSegments{ sdcel_prime } }
  }

  def overlayMaster(
    ldcelA: RDD[(Half_edge, String, Envelope, Polygon)], ma: Map[String, EmptyCell],
    ldcelB: RDD[(Half_edge, String, Envelope, Polygon)], mb: Map[String, EmptyCell])
    (implicit cells: Map[Int, Cell], geofactory: GeometryFactory, settings: Settings,
      spark: SparkSession): List[(Polygon, String)] = {

    val sdcel_prime = ldcelA.zipPartitions(ldcelB,
      preservesPartitioning=true){ (iterA, iterB) =>

      val pid = TaskContext.getPartitionId
      val cell = cells(pid)

      val A = getFaces(iterA, cell, ma)
      val B = getFaces(iterB, cell, mb)

      val hedges = merge(A, B, cells)
      hedges.toIterator
    }
    
    val segments = collectSegments{ sdcel_prime }
      .map{ case(seg, lab) => (Segment.save(seg), lab) }
      .collect
      .map{ case(seg, lab) => (Segment.load(seg), lab) }
    
    val faces = mergeSegmentsByLevel(segments)
      .map{ case(label, coords) =>
        label -> coords.filter(_.getCoords.size >= 3).map{c =>
          geofactory.createPolygon(c.getCoords)
        }.head
      }

    if(settings.debug){
      save("/tmp/edgesP.wkt"){
        faces.map{ case(label, face) =>
          s"${face.toText}\t$label\n"
        }.toList
      }
    }

    faces.toList.map(_.swap)
  }

  def overlayByLevel(
    ldcelA: RDD[(Half_edge, String, Envelope, Polygon)], ma: Map[String, EmptyCell],
    ldcelB: RDD[(Half_edge, String, Envelope, Polygon)], mb: Map[String, EmptyCell])
    (implicit cells: Map[Int, Cell], geofactory: GeometryFactory, settings: Settings,
      spark: SparkSession): Unit = {

    val sdcel_prime = ldcelA.zipPartitions(ldcelB,
      preservesPartitioning=true){ (iterA, iterB) =>

      val pid = TaskContext.getPartitionId
      val cell = cells(pid)

      val A = getFaces(iterA, cell, ma)
      val B = getFaces(iterB, cell, mb)

      val hedges = merge(A, B, cells)
      hedges.toIterator
    }

    val segs1 = collectSegments{ sdcel_prime}
    val (segs2, m) = aggregateSegments( segs1, settings.level)

    val S = segs2.mapPartitions{ it =>
      mergeSegmentsByLevel(it.toArray).toIterator
    }.collect
    val segs3 = S.map{ case(label, coords)  =>
      coords.map{ C => (Segment(C.getHedges), label)}
    }.flatten
    val r2 = mergeSegmentsByLevel(segs3).map{ case(label, coords) =>
      label -> coords.filter(_.getCoords.size >= 3).map{c =>
        geofactory.createPolygon(c.getCoords)
      }.head
    }
    save("/tmp/edgesP.wkt"){
      r2.map{ case(label, polygon) =>
        s"${polygon.toText}\t$label\n"
      }.toList
    }
    
  }

  /*  Merge by cells  */
  def mergeSegmentsByLevel(segments: Array[(Segment, String)])
    (implicit geofactory: GeometryFactory, settings: Settings):
      Map[String, Array[Coords]] = {

    if(settings.debug){
      val pid = TaskContext.getPartitionId
      logger.info(s"mergeSegments|$pid|${segments.size}")
    }
    val reader = new WKTReader(geofactory)

    segments.groupBy(_._2).map{ case(label, segs) =>
      val lines_prime = segs.map(_._1.toLine)
      val coords = lines_prime.map{ line =>
        val arr = line.split("\t")
        val wkt     = arr(0)
        val isHole  = arr(1) // Is part of a hole?
        val isValid = arr(2) // Do I need to remove a coordinate later?
        val linestring = reader.read(wkt).asInstanceOf[LineString]
        linestring.setUserData(s"$isHole\t$isValid")
        val coords = linestring.getCoordinates

        val start  = coords.head
        val end    = coords.last
        val middle = coords.slice(1, coords.size - 1).map(c => Coord(c, true))

        val coords_prime = isValid match {
          case "B" => Coord(start, false) +: middle :+ Coord(end, false)
          case "S" => Coord(start, false) +: middle :+ Coord(end, true)
          case "E" => Coord(start, true)  +: middle :+ Coord(end, false)
          case  _  => Coord(start, true)  +: middle :+ Coord(end, true)
        }
        Coords(coords_prime)
      }.toSet

      label -> reduceCoords(coords)
    }
  }

  def reduceCoords(coords: Set[Coords])
    (implicit geofactory: GeometryFactory, settings: Settings): Array[Coords] = {

    mergeCoords(coords).toArray
  }

  def mergeCoords(coords: Set[Coords])
    (implicit geofactory: GeometryFactory): List[Coords] = {

    @tailrec
    def recursion(coords: Set[Coords], curr: Coords, r: List[Coords]): List[Coords] = {
      if(coords.isEmpty){
        r :+ curr
      } else if(curr.isClose){
        val new_r      = r :+ curr
        val new_curr   = coords.head
        val new_coords = coords.tail
        recursion(new_coords, new_curr, new_r)
      } else {
        val (a, b, c) = if(coords.exists(c => curr.touch(c.first.coord))) {
          val next = coords.filter(c => curr.touch(c.first.coord)).head
          val new_coords = coords -- Set(next)
          val new_curr   = Coords(curr.coords ++ next.coords.tail)
          (new_coords, new_curr, r)
        } else {
          val new_r      = r :+ curr
          val new_curr   = coords.head
          val new_coords = coords.tail
          (new_coords, new_curr, new_r)
        }
        recursion(a, b, c)
      }
    }

    recursion(coords.tail, coords.head, List.empty[Coords])
  }

  /*  Merge by label  */
  def mergeSegments(sdcel: RDD[(Segment, String)])
    (implicit geofactory: GeometryFactory, settings: Settings): RDD[(String, Polygon)] = {

    val a = sdcel//.filter{!_._1.isClose}
      .map{ case(s,l) => (l,List(s.getLine)) }
      .reduceByKey{ case(a, b) => a ++ b }.persist(settings.persistance)

    if(settings.debug){
      save(s"/tmp/edgesS.wkt"){
        a.mapPartitionsWithIndex{ (pid, it) =>
          it.flatMap{ case(label,segs) =>
            segs.map{ seg =>
              s"$seg\t$label\t$pid\n"
            }
          }
        }.collect
      }
    }

    val b = a.mapPartitionsWithIndex{ (pid, it) =>
        val reader = new WKTReader(geofactory)
        it.flatMap{ case(label, segs_prime) =>          
          val segs = segs_prime.map{ s =>
            val arr = s.split("\t")
            val s1 = arr(0)
            val s2 = arr(1) // is part of a hole?
            val s3 = arr(2) // I need to remove a coordinate later...
            val seg = reader.read(s1).asInstanceOf[LineString]
            seg.setUserData(s"$s2\t$s3") // ishole, flag
            seg
          }
          val polygons = mergeLocalSegments(segs, label)
          polygons.map{ polygon => (label, polygon) }.groupBy(_._1).map{ case(lab, list) =>
            val polys = list.map(_._2).sortBy(_.getArea)(Ordering[Double].reverse)
            
            val outer0 = polys.head.getExteriorRing.getCoordinates
            val outer = outer0
            val shell = geofactory.createLinearRing(outer)
            val O = geofactory.createPolygon(shell)

            val inners = polys.tail.map{ poly =>
              val inner0 = poly.getExteriorRing.getCoordinates
              val inner = inner0
              geofactory.createLinearRing(inner)
            }.toArray

            val (holes, noholes) = inners.partition{ i =>
              i.getInteriorPoint.coveredBy(O)
            }

            val p = geofactory.createPolygon(shell, holes)
            val p2 = noholes.map{ nh =>
              geofactory.createPolygon(nh)
            }
            val ps = p +: p2

            ps.map{ p => (lab, p)}
          }.flatten
        }
      }
    b
  }

  def mergeLocalSegments(segments: List[LineString], label:String = "")
    (implicit geofactory: GeometryFactory, settings: Settings): List[Polygon] = {

    val coords = segments.map{ line =>
      val flag = {
        val arr = line.getUserData.toString.split("\t")
        try{
          arr(1)
        } catch {
          case e: java.lang.ArrayIndexOutOfBoundsException => ""
        }
      }

      val coords = line.getCoordinates
      val start = coords.head
      val end = coords.last
      val middle = coords.slice(1, coords.size - 1).map(c => Coord(c, true))
      val mycoords = flag match {
        case "B" => Coord(start, false) +: middle :+ Coord(end, false)
        case "S" => Coord(start, false) +: middle :+ Coord(end, true)
        case "E" => Coord(start, true) +: middle :+ Coord(end, false)
        case  _  => Coord(start, true) +: middle :+ Coord(end, true)
      }
      Coords(mycoords)
    }.toSet

    val C = try{
      mergeCoordinates(coords.tail, coords.head, List.empty[Coords])
    } catch {
      case e: java.lang.UnsupportedOperationException => {
        coords.foreach{println}
        List.empty[Coords]
      }
    }

    C.filter(_.getCoords.size >= 4).map{c =>
      val poly = geofactory.createPolygon(c.getCoords)
      poly
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
        coords.filter(c => curr.touch(c.first.coord)).head
      } catch {
        case e: java.util.NoSuchElementException => {
          logger.info(s"NoSuchElementException")
          coords.map{ c =>
            val d = curr.last.coord.distance(c.first.coord)
            (c, d)
          }.toList.sortBy(_._2).map(_._1).head
        }
      }
      val n_coords = coords -- Set(next)
      val n_curr = Coords(curr.coords ++ next.coords.tail)
      mergeCoordinates(n_coords, n_curr, r)
    }
  }

  /*  Merge by polygons  */
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

  /*  Collect segments by parittion  */
  def collectSegments(sdcel: RDD[(Half_edge, String)])
    (implicit geofactory: GeometryFactory, settings: Settings): RDD[(Segment, String)] = {

    if(settings.debug){
      save("/tmp/edgesZ.wkt"){
        sdcel.mapPartitionsWithIndex{ (pid, it) =>
          it.map{ case(hedge, label) =>
            val wkt = hedge.getPolygon.toText
            
            s"$wkt\t$label\n"
          }
        }.collect
      }
    }
    
    val a = sdcel.mapPartitionsWithIndex{ (pid, it) =>
      it.flatMap{ case(hedge, label) =>

        if(hedge.isValid){
          val s = getValidSegments(hedge, hedge.prev, List(hedge), List.empty[Segment])
          s.map(h => (h, label))
        } else {
          val valid = getNextValidOrStop(hedge.next, hedge, hedge)
          if(valid == hedge){
            List.empty[(Segment, String)]
          } else {
            val s = getValidSegments(valid, valid.prev, List(valid), List.empty[Segment])
            s.map(h => (h, label))
          }
        }
      }
    }
    a
  }

  @tailrec
  def getValidSegments(hcurr: Half_edge, hstop: Half_edge, listH: List[Half_edge],
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
      if(hcurr.next != null && hcurr.next.isValid){
        val next = hcurr.next
        getValidSegments(next, hstop, listH :+ next, listS,p)
      } else {
        val valid = getNextValidOrStop(hcurr.next, hcurr, hstop)
        hcurr.next = null
        if(valid == hstop){
          valid.next = null
          if(valid.isValid){
            val list = listH //:+ hcurr
            val s1 = Segment(list)
            val s2 = Segment(List(valid))
            listS ++ List(s1,s2)
          } else {
            val s = Segment(listH)
            listS :+ s
          }
        } else {
          val s = Segment(listH)
          getValidSegments(valid, hstop, List(valid), listS :+ s,p)
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
    } else if(curr != null && curr.isValid){
      curr
    } else {
      getNextValidOrStop(curr.next, stop1, stop2)
    }
  }

  /*  Overlay operators  */
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
}
