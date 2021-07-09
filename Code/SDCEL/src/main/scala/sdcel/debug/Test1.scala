package edu.ucr.dblab.sdcel.debug

import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory,LineString}
import com.vividsolutions.jts.geom.Polygon
import com.vividsolutions.jts.io.WKTReader
import com.vividsolutions.jts.algorithm.CGAlgorithms

import edu.ucr.dblab.sdcel.geometries.{Half_edge, Vertex, EdgeData, HEdge}
import edu.ucr.dblab.sdcel.DCELMerger2.merge2
import scala.io.Source
import scala.annotation.tailrec

object Test1 {
  def main(args: Array[String]): Unit = {
    val model = new PrecisionModel(1000.0)
    implicit val geofactory = new GeometryFactory(model)
    implicit val reader = new WKTReader(geofactory)

    val borders = List(
      List(3,4,5),
      List(1,2,3),
      List(6,7,8),
      List(8,9,0)
    )

    for(i <- 0 until borders.length){
      val c = checkExtension(borders(i), borders)
      println(s"has ${borders(i)} an extension? $c")
    }

    for(i <- 0 until borders.length){
      if(checkExtension(borders(i), borders)){
        val extension = extend(borders(i), borders)
        println(s"${borders(i)} extension is $extension")
      }
    }

    val r = findExtensions(borders, List.empty[List[Int]])
    println("The result is:")
    r.foreach { println }

    val a = List(
      "LINESTRING (2681872.697 229510.179, 2681857.981 229437.579)",
      "LINESTRING (2681857.981 229437.579, 2682655.416 229437.579)",
      "LINESTRING (2682655.416 229437.579, 2682982.063 229788.444)")
      .map{ x =>
        val line = reader.read(x).asInstanceOf[LineString]
        line.setUserData(EdgeData(33,0,-1,false))
        Half_edge(line)
      }

    val b = List(
      "LINESTRING (2682982.063 229788.444, 2682655.416 229437.579)",
      "LINESTRING (2682655.416 229437.579, 2682982.063 229437.579)",
      "LINESTRING (2682982.063 229437.579, 2682982.063 229788.444)")
      .map{ x =>
        val line = reader.read(x).asInstanceOf[LineString]
        line.setUserData(EdgeData(34,0,-1,false))
        Half_edge(line)
      }

    val c = List(
      "LINESTRING (2682655.416 229437.579, 2682982.063 229788.444)",
      "LINESTRING (2682982.063 229788.444, 2682982.063 232529.764)",
      "LINESTRING (2682982.063 232529.764, 2682643.216 232470.823)")
      .map{ x =>
        val line = reader.read(x).asInstanceOf[LineString]
        line.setUserData(EdgeData(33,0,-1,false))
        Half_edge(line)
      }

    val segments = List(a,b,c)
    val s = findExtensions(segments, List.empty[List[Half_edge]])
    println("The result is:")
    s.zipWithIndex.map{ case(x, i) => x.map(l => s"${l.toString}\t$i")}.flatten.foreach { println }
    
  }

  def checkExtension[T](x: List[T], borders: List[List[T]]): Boolean = {
    borders.exists { b  => b.last == x.head || b.head == x.last }
  }

  def extend[T](x: List[T], borders: List[List[T]]): List[T] = {
    val left  = borders.foldLeft(x){ (l, r) => if(l.head == r.last) r ++ l.tail else l }
    val right = borders.foldLeft(x){ (l, r) => if(l.last == r.head) l ++ r.tail else l }

    left.slice(0, left.length - x.length) ++ right
  }

  @tailrec
  def findExtensions[T](borders: List[List[T]], result: List[List[T]])
      : List[List[T]] = {

    val (extendibles, isolates) = borders.partition( b => checkExtension(b, borders))
    val new_result = result ++ isolates
    if(extendibles.isEmpty){
      new_result
    } else {
      val first = extendibles.head
      val tail  = extendibles.tail
      val extension = extend(first, tail)

      val new_borders = extendibles.filter{ x =>
        extension.intersect(x).isEmpty } ++ List(extension)

      findExtensions(new_borders, new_result)
    }
  }
}
