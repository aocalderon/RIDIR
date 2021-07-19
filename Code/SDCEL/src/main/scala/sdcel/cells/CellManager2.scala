package edu.ucr.dblab.sdcel.cells

import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.geom.{Geometry, Envelope, Coordinate, Point, Polygon}

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer

import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator

import scala.collection.JavaConverters._
import scala.annotation.tailrec

import edu.ucr.dblab.sdcel.PartitionReader._
import edu.ucr.dblab.sdcel.Utils.{Settings, save}
import edu.ucr.dblab.sdcel.quadtree.{StandardQuadTree, QuadRectangle, Quadtree}
import edu.ucr.dblab.sdcel.geometries.{Cell, Half_edge}

object EmptyCellManager {
  // model the data from a empty cell:
  // point: the reference point to find the required polygon id.
  // pid: the partition id where the point must be queried.
  // empty: the partition id of the empty cell.
  // polyId: the polygon id which the empty cell must be updated.
  case class EmptyCell(point: Point, pid: Int, empty:Int,
    polyId: Int = -1, label: String = "*"){

    override def toString: String = s"$point\t$pid\t$empty\t$polyId\t$label"

    // return a expanded version of the point to help in the intersection call...
    def reference(implicit settings: Settings, geofactory: GeometryFactory): Polygon = {
      val envelope = point.getEnvelopeInternal
      envelope.expandBy(settings.tolerance)
      envelope2polygon(envelope)
    }
  }

  def fixEmptyCells(r: List[EmptyCell], sdcel: RDD[(Half_edge, String)],
    cells: Map[Int, Cell])
    (implicit settings: Settings, geofactory: GeometryFactory):  RDD[(Half_edge, String)]= {

    if(!r.isEmpty){
      val pids = r.map(_.empty).toSet

      sdcel.mapPartitionsWithIndex{ (pid, it) =>
        if(pids.contains(pid)){
          val empty = r.filter(_.empty == pid).head
          
          val h = cells(pid).toHalf_edge(empty.polyId, empty.label)
          val tuple = (h, empty.label)
          it ++ Iterator(tuple)
        } else {
          it
        }
      }
    } else {
      sdcel
    }
  }  

  def updatePolygonIds(r: List[EmptyCell], sdcel: RDD[(Half_edge, String)])
      (implicit settings: Settings, geofactory: GeometryFactory): List[EmptyCell] = {
    val pids = r.map(_.pid).toSet
    sdcel.mapPartitionsWithIndex{ (pid, it) =>
      if(pids.contains(pid)){
        val cells  = r.filter(_.pid == pid)
        val hedges = it.toList

        val list = for{
          cell  <- cells
          hedge <- hedges if hedge._1.getPolygon.intersects{cell.reference}
        } yield {
          cell.copy(polyId = hedge._1.getPolygonId, label = hedge._2)
        }
        list.toIterator
      } else {
        List.empty[EmptyCell].toIterator
      }
    }.collect.toList
  }

  def solve[T](quadtree: StandardQuadTree[T], cells: Map[Int, Cell],
    non_empties: List[Int], empties: List[Int])
    (implicit geofactory: GeometryFactory, settings: Settings): List[EmptyCell] = {

    solveRec(quadtree, cells, empties, non_empties, List.empty[(Point,Int,Int)])
      .distinct
      .map{ case(point, pid, empty) => EmptyCell(point, pid, empty)}
  }

  @tailrec
  private def solveRec[T](quadtree: StandardQuadTree[T], cells: Map[Int, Cell],
    empties: List[Int], non_empties: List[Int], result: List[(Point, Int, Int)])
    (implicit geofactory: GeometryFactory, settings: Settings): List[(Point, Int, Int)] = {

    // iterate recursively over the set of empty cells...
    println("Cell")
    println(empties.head)
    empties match {
      case Nil => result
      case empty +: tail => {
        val cell = cells(empty)
        val (pid, point, pids) = getClosestPath(quadtree, non_empties, cell)
        // remove the complete path of cells solved in the past call...
        val new_empties = tail.toSet.diff(pids.toSet).toList
        val new_result = result ++ pids.map( p => (point, pid, p))
        solveRec(quadtree, cells, new_empties, non_empties, new_result)
      }
    }
  }

  // helper function to call closest function
  private def getClosestPath[T](Q: StandardQuadTree[T], NE: List[Int], c: Cell)
    (implicit geofactory: GeometryFactory, settings: Settings):
      (Int, Point, List[Int]) = {

    // just create a fake point and empty list to start the recursion...
    val point_prime = geofactory.createPoint(new Coordinate(0,0))
    val path_prime  = List.empty[Int]
    val (pid, path, point) = closest(Q, NE, c, path_prime, point_prime)

    (pid, point, path :+ c.id) // attach the current cell to the path
  }

  @tailrec
  // Recursive function to get the list of empty cells and the non-empty cell and point
  // which is the closest to them...
  private def closest[T](Q: StandardQuadTree[T], NE: List[Int], c: Cell,
    path: List[Int], point: Point)
    (implicit geofactory: GeometryFactory, settings: Settings):
      (Int, List[Int], Point) = {

    // get cells in the corner and reference point...
    val (cs, point) = getCellsAtCorner(Q, c)
    // if one of the cells is non-empty we can finish...
    if(cs.exists(x => isInNonEmpties(x, NE))){
      val (non_empties, empties) = cs.partition(x => isInNonEmpties(x, NE))
      val non_empty = non_empties.head.id
      val ids = empties.map(_.id)
      (non_empty, path ++ ids, point)
    } else { // if not...
      val new_c = getDeepestCell(cs) // choose the deepest cell...
      // extract the cell parent quadtree...  
      val Q_prime = Quadtree.extract(Q, getLineageParent(new_c))
      // filter the non-empty cells to only the children of the choosen cell parent...
      val new_NE = filterNonEmpties(NE, Q_prime) 
      val new_path = path ++ cs.map(_.id) // accumulate the set of empty cells involved...
      // repeat recursion...
      closest(Q, new_NE, new_c, new_path, point)
    }
  }

  // Return the 3 cells that touch the internal corner of the given cell...
  // The internal corner is that one which point to the interior
  // of the cell parent...
  private def getCellsAtCorner[T](quadtree: StandardQuadTree[T], c: Cell)
    (implicit geofactory: GeometryFactory, settings: Settings): (List[Cell], Point) = {

    // take the quadrant of the cell...
    val region = c.lineage.takeRight(1).toInt
    val b = c.boundary
    // depending on quadrant and boundary get the coordinate of interior corner...
    val (x, y) = region match {
      case 0 => (b.getMaxX, b.getMinY)
      case 1 => (b.getMinX, b.getMinY)
      case 2 => (b.getMaxX, b.getMaxY)
      case 3 => (b.getMinX, b.getMaxY)
    }
    val corner = geofactory.createPoint(new Coordinate(x, y))
    val envelope = corner.getEnvelopeInternal
    envelope.expandBy(settings.tolerance) // expand corner a bit to ensure touch...

    // from quadtree get the cells that touch the corner...
    val cells = quadtree.findZones(new QuadRectangle(envelope)).asScala
      .filterNot(_.lineage == c.lineage) // remove current cell...
      .map{ q =>
        val id = q.partitionId.toInt
        Cell(id, q.lineage, envelope2ring(q.getEnvelope))
      }.toList

    (cells, corner)
  }

  // Return the lineage of this cell parent...
  private def getLineageParent(cell: Cell): String =
    cell.lineage.substring(0, cell.lineage.length - 1)

  // Return the cell with the deepest lineage in the list...
  private def getDeepestCell(cells: List[Cell]): Cell = 
    cells.map(cell => (cell.lineage.length, cell)).maxBy(_._1)._2

  // Filter the list of non-empty cells with those from the new quadtree...
  private def filterNonEmpties[T](non_empties: List[Int],
    quadtree: StandardQuadTree[T]): List[Int] = {

    val ids = quadtree.getLeafZones.asScala.map(_.partitionId)
    non_empties.filter(id => ids.contains(id))
  }

  // Test if the cell is in the non-empty list...
  private def isInNonEmpties(cell: Cell, non_empties: List[Int]): Boolean =
    non_empties.exists(_ == cell.id)

  // for testing purposes...
  def main(args: Array[String]) = {
    implicit val geofactory = new GeometryFactory(new PrecisionModel(100))
    implicit val settings = Settings(tolerance = 0.001, debug = false)
    implicit val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()
    import spark.implicits._

    val qpath = args(0)
    val bpath = args(1)
    val (quadtree, cells) = readQuadtree[Int](qpath, bpath)

    // Testing quadtree creation...
    save("/tmp/edgesQ.wkt"){
      cells.values.map{ cell =>
        cell.wkt + "\n"
      }.toList
    }

    // Testing getCellsAtCorner function...
    save("/tmp/edgesC.wkt"){
      quadtree.getLeafZones.asScala.map{ leaf =>
        val id = leaf.partitionId
        val cell = Cell(id, leaf.lineage, envelope2ring(leaf.getEnvelope))
        val (around, point) = getCellsAtCorner(quadtree, cell)

        val wkt = point.toText
        val ids = around.map(_.id).mkString(" ")

        s"$wkt\t$id: $ids\n"
      }
    }

    // Testing quadtree extract function...
    val lineageId = "10"
    save(s"/tmp/edgesL.wkt"){
      Quadtree.extract(quadtree, lineageId).getLeafZones.asScala.map{ leaf =>
        val id = leaf.partitionId
        val lineage = leaf.lineage
        val wkt = envelope2polygon(leaf.getEnvelope).toText
        s"$wkt\t$id\t$lineage\n"
      }
    }

    // Testing getClosestPath function...
    val non_empties = List(5, 17, 32, 48)
    val empties = (0 to 51).toSet.diff(non_empties.toSet).toList
    //val result = solveEmptyCells(quadtree, cells, non_empties)
    val result = solve(quadtree, cells, non_empties , empties)
    save("/tmp/edgesR.wkt"){
      result.map{ cell =>
        val wkt = cells(cell.empty).wkt
        s"$wkt\t${cell.empty}\t${cell.pid}\n"
      }.toList
    }

    spark.close()
  }
}

