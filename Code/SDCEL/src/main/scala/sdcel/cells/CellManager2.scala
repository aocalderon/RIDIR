package edu.ucr.dblab.sdcel.cells

import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.geom.{Geometry, Envelope, Coordinate, Point, Polygon}

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer

import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator

import scala.collection.immutable.HashMap
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
    polyId: Long = -1, label: String = "*"){

    override def toString: String = s"$point\t$pid\t$empty\t$polyId\t$label"

    // return a expanded version of the point to help in the intersection call...
    def reference(implicit settings: Settings, geofactory: GeometryFactory): Polygon = {
      val envelope = point.getEnvelopeInternal
      envelope.expandBy(settings.tolerance)
      envelope2polygon(envelope)
    }
  }

  def runEmptyCells[T](sdcel: RDD[(Half_edge, String)], quadtree: StandardQuadTree[T], cells: Map[Int, Cell])
    (implicit geofactory: GeometryFactory, settings: Settings): RDD[(Half_edge, String)]= {

    val (ne, e) = sdcel.mapPartitionsWithIndex{ (pid, it) =>
      val faces = it.map(_._1.getPolygon).toList
      val cell = cells(pid).mbr
      val non_empty = faces.exists{ _.intersects(cell) }
      val r = (pid, non_empty)
      Iterator(r)
    }.collect().partition{ case(pid, non_empty) => non_empty }

    val non_empties = ne.map(_._1).toList
    val empties = e.map(_._1).toList

    val empties_prime = cells.filter{ case(id, cell) => empties.contains(id) }
      .values.map(cell => (cell.lineage, cell)).toList
    val emptiesHash = HashMap(empties_prime.map( i => i._1 -> i._2): _* )
    val nempties_prime = cells.filter{ case(id, cell) => non_empties.contains(id) }
      .values.map(cell => (cell.lineage, cell)).toList
    val nemptiesHash = HashMap(nempties_prime.map( i => i._1 -> i._2): _* )
    val (quadtree_prime, empties3) = cleanQuadtree(quadtree, emptiesHash, nemptiesHash)

    save("/tmp/edgesEM.wkt"){
      empties3.values.toList.map{ cell =>
        val wkt = cell.wkt
        val id = cell.id
        val lineage = cell.lineage

        s"$wkt\t$id\t$lineage\n"
      }
    }

    val lineage2id = quadtree_prime.getLeafZones.asScala.map{ leaf =>
      val id = leaf.partitionId.toInt
      val lineage = leaf.lineage
      lineage -> id
    }.toMap
    val id2lineage = quadtree_prime.getLeafZones.asScala.map{ leaf =>
      val id = leaf.partitionId.toInt
      val lineage = leaf.lineage
      id -> lineage
    }.toMap

    val empties_prime2  = empties3.keys.map(key => lineage2id(key)).toList
    val cells_prime = quadtree_prime.getLeafZones.asScala.map{ leaf =>
      val ring = envelope2ring(leaf.getEnvelope)
      val id = leaf.partitionId
      val lineage = leaf.lineage
      id.toInt -> Cell(id, lineage, ring)
    }.toMap
    val nempties_prime2 = ((0 to quadtree_prime.getLeafZones.size).toSet -- empties_prime2.toSet).toList

    val r_prime = solve(quadtree_prime, cells_prime, nempties_prime2, empties_prime2)
    println("Solve")
    println(s"r_prime size = ${r_prime.size}")
    val pids = r_prime.map(r => id2lineage(r.pid)).distinct.sorted
    println(s"pids size = ${pids.size}")

    //r_prime.take(5).foreach(println)


    // Getting polygon IDs from known partitions...
    val r = updatePolygonIds(r_prime, sdcel, id2lineage, cells)
    val str = r.map{_.toString}.mkString("\n")
    println(s"r:\n$str")

    val sdcel_prime = fixEmptyCells(r, sdcel, cells)

    sdcel_prime
/*
    save("/tmp/edgesNQ.wkt"){
      quadtree_prime.getLeafZones.asScala.map{ leaf =>
        val wkt = envelope2polygon(leaf.getEnvelope).toText
        val id = leaf.partitionId
        val lineage = leaf.lineage

        s"$wkt\t$id\t$lineage\n"
      }
    }
    save("/tmp/edgesEM.wkt"){
      empties3.values.toList.map{ cell =>
        val wkt = cell.wkt
        val id = cell.id
        val lineage = cell.lineage

        s"$wkt\t$id\t$lineage\n"
      }
    }

    //sdcel
 */
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

  def updatePolygonIds(r: List[EmptyCell], sdcel: RDD[(Half_edge, String)],
    id2lineage: Map[Int, String], cells: Map[Int, Cell])
      (implicit settings: Settings, geofactory: GeometryFactory): List[EmptyCell] = {
    val pids = r.map(r => id2lineage(r.pid)).toSet
    println(s"pids size = ${pids.size}")
    sdcel.mapPartitionsWithIndex{ (pid, it) =>
      val lineage = cells(pid).lineage
      if(pids.filter(_.size <= lineage.size).exists(l => lineage.substring(0, l.size-1) == l)){
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
      .map{ case(point, pid, empty) => EmptyCell(point, pid, empty) }
  }

  @tailrec
  private def solveRec[T](quadtree: StandardQuadTree[T], cells: Map[Int, Cell],
    empties: List[Int], non_empties: List[Int], result: List[(Point, Int, Int)])
    (implicit geofactory: GeometryFactory, settings: Settings): List[(Point, Int, Int)] = {

    // iterate recursively over the set of empty cells...
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

  def groupByLineage(emptyCells: Vector[Cell])(implicit geofactory: GeometryFactory)
      : (Vector[Cell], Vector[Cell])= {
    val (a, b) = emptyCells
      .map{ cell =>
        val parent_lineage = cell.lineage.reverse.tail.reverse
        (parent_lineage, cell)
      }
      .groupBy(_._1)
      .map{ case(parent, cells) =>
        val children = cells.map(_._2).toList

        (parent, children.length, children)
      }.partition(_._2 != 4)

    val keep_them = a.flatMap(_._3).toVector

    val merge_them = b.toVector.map{ children =>
      val lineage = children._1
      val ids = children._3.map{_.id}
      val boundary = children._3.map(_.boundary)
        .reduce{ (a, b) =>
          a.expandToInclude(b)
          a
        }
      Cell(-1, lineage, envelope2ring(boundary))
    }

    (merge_them, keep_them)
  }

  @tailrec
  def mergeEmptyCells(empties: Map[Int, Vector[Cell]],
    levels: List[Int],
    previous: Vector[Cell],
    accum: Vector[Cell])(implicit geofactory: GeometryFactory): Vector[Cell] = {

    levels match {
      case Nil => accum
      case level :: tail => {
        val current = empties(level)
        val E = current ++ previous
        val X = groupByLineage(E)
        val M = X._1
        val K = X._2

        mergeEmptyCells(empties, levels.tail, M, K ++ accum)
      }
    }
  }

  def cleanQuadtree[T](original_quadtree: StandardQuadTree[T],
    empties: HashMap[String, Cell], nonempties: HashMap[String, Cell])
    (implicit geofactory: GeometryFactory):
      (StandardQuadTree[T], HashMap[String, Cell]) = {

    val emptiesByLevel = empties.values.map(cell => (cell.lineage.length(), cell))
      .groupBy(_._1)
      .map(g => g._1 -> g._2.map(_._2).toVector)
      .withDefaultValue(Vector.empty[Cell])

    val levels = (1 to emptiesByLevel.keys.max).toList.reverse
    
    val previous = Vector.empty[Cell]
    val accum = Vector.empty[Cell]

    val mergedCells = mergeEmptyCells(emptiesByLevel, levels, previous, accum)
    val empties_prime = HashMap(mergedCells.map(cell => cell.lineage -> cell): _*) 

    val boundary = original_quadtree.getZone().getEnvelope()
    val lineages = (nonempties.values ++ empties_prime.values)
      .map(_.lineage).toList.sorted

    val quadtree_prime = Quadtree.create[T](boundary, lineages)

    (quadtree_prime, empties_prime)
  }  

  // for testing purposes...
  def main(args: Array[String]) = {
    implicit val geofactory = new GeometryFactory(new PrecisionModel(1000.0))
    implicit val settings = Settings(tolerance = 0.001, debug = false)
    implicit val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()
    import spark.implicits._

/*
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
 */

    // Testing clean quadtree...
    import scala.io.Source
    val epath  = "/home/acald013/RIDIR/local_path/CA/P3000/empties.txt"
    val nepath = "/home/acald013/RIDIR/local_path/CA/P3000/nonempties.txt"
    val empties2  = Source.fromFile(epath).getLines.map(_.toInt).toList
    val nempties2 = Source.fromFile(nepath).getLines.map(_.toInt).toList
    val qpath2 = "/home/acald013/RIDIR/local_path/CA/P3000/quadtree.wkt"
    val bpath2 = "/home/acald013/RIDIR/local_path/CA/P3000/boundary.wkt"

    val (quadtree_test, cells_test) = readQuadtree[Int](qpath2, bpath2)

    val empties_prime = cells_test.filter{ case(id, cell) => empties2.contains(id) }
      .values.map(cell => (cell.lineage, cell)).toList
    val emptiesHash = HashMap(empties_prime.map( i => i._1 -> i._2): _* )
    val nempties_prime = cells_test.filter{ case(id, cell) => nempties2.contains(id) }
      .values.map(cell => (cell.lineage, cell)).toList
    val nemptiesHash = HashMap(nempties_prime.map( i => i._1 -> i._2): _* )

    val (new_quadtree, empties3) = cleanQuadtree(quadtree_test, emptiesHash, nemptiesHash)

    save("/tmp/edgesNQ.wkt"){
      new_quadtree.getLeafZones.asScala.map{ leaf =>
        val wkt = envelope2polygon(leaf.getEnvelope).toText
        val id = leaf.partitionId
        val lineage = leaf.lineage

        s"$wkt\t$id\t$lineage\n"
      }
    }

    save("/tmp/edgesEM.wkt"){
      empties3.values.toList.map{ cell =>
        val wkt = cell.wkt
        val id = cell.id
        val lineage = cell.lineage

        s"$wkt\t$id\t$lineage\n"
      }
    }

    save("/tmp/empties3.txt"){
      empties3.values.toList.map{ cell =>
        val id = cell.id

        s"$id\n"
      }
    }

    println(empties2.size)
    println(empties3.size)

    spark.close()
  }
}

