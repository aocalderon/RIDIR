package edu.ucr.dblab.sdcel.cells

import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.geom.{Geometry, Envelope, Coordinate, Point}

import org.apache.spark.sql.SparkSession
import org.apache.spark.serializer.KryoSerializer

import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator

import scala.collection.JavaConverters._
import scala.annotation.tailrec

import edu.ucr.dblab.sdcel.PartitionReader._
import edu.ucr.dblab.sdcel.Utils.{Settings, save}
import edu.ucr.dblab.sdcel.quadtree.{StandardQuadTree, QuadRectangle, Quadtree}

object CellManager2 {
  // Model the cell of a quadtree...
  case class Cell(id: Int, lineage: String, boundary: Envelope)  
  // Model the relation between an 'empty' cell and its closest 'non-empty' cell...
  case class ClosestCell(lineage: String, closest: String, point: Point)

  def getClosestPath[T](Q: StandardQuadTree[T], NE: List[Int], c: Cell)
    (implicit geofactory: GeometryFactory, settings: Settings):
      (Int, Point, List[Int]) = {

    val point_prime = geofactory.createPoint(new Coordinate(0,0))
    val path_prime  = List.empty[Int]
    val (pid, path, point) = closest(Q, NE, c, path_prime, point_prime)

    (pid, point, path :+ c.id)
  }

  @tailrec
  private def closest[T](Q: StandardQuadTree[T], NE: List[Int], c: Cell,
    path: List[Int], point: Point)
    (implicit geofactory: GeometryFactory, settings: Settings):
      (Int, List[Int], Point) = {

    //println(s"c: $c")
    val (cs, point) = getCellsAtCorner(Q, c)
    //println(s"cs: $cs")
    //println(s"point: $point")
    if(cs.exists(x => isInNonEmpties(x, NE))){
      val (non_empties, empties) = cs.partition(x => isInNonEmpties(x, NE))
      val non_empty = non_empties.head.id
      val ids = empties.map(_.id)
      (non_empty, path ++ ids, point)
    } else {
      val new_c = getDeepestCell(cs)
      //println(s"new_c: $new_c")

      val Q_prime = Quadtree.extract(Q, getLineageParent(new_c))
      val new_NE = filterNoNEmpties(NE, Q_prime)
      val new_path = path ++ cs.map(_.id)
      closest(Q, new_NE, new_c, new_path, point)
    }
  }

  private def getCellsAtCorner[T](quadtree: StandardQuadTree[T], c: Cell)
    (implicit geofactory: GeometryFactory, settings: Settings): (List[Cell], Point) = {

    val region = c.lineage.takeRight(1).toInt
    val b = c.boundary
    val (x, y) = region match {
      case 0 => (b.getMaxX, b.getMinY)
      case 1 => (b.getMinX, b.getMinY)
      case 2 => (b.getMaxX, b.getMaxY)
      case 3 => (b.getMinX, b.getMaxY)
    }
    val corner = geofactory.createPoint(new Coordinate(x, y))
    val envelope = corner.getEnvelopeInternal
    envelope.expandBy(settings.tolerance)
    val cells = quadtree.findZones(new QuadRectangle(envelope)).asScala
      .filterNot(_.lineage == c.lineage)
      .map{ q =>
        val id = q.partitionId.toInt
        Cell(id, q.lineage, q.getEnvelope)
      }.sortBy(_.id).toList

    (cells, corner)
  }

  // Return the lineage of this cell parent...
  private def getLineageParent(cell: Cell): String =
    cell.lineage.substring(0, cell.lineage.length - 1)

  // Return the cell with the deepest lineage in the list...
  private def getDeepestCell(cells: List[Cell]): Cell = 
    cells.map(cell => (cell.lineage.length, cell)).maxBy(_._1)._2

  // Filter the list of non-empty cells with those from the new quadtree...
  private def filterNoNEmpties[T](non_empties: List[Int],
    quadtree: StandardQuadTree[T]): List[Int] = {

    val ids = quadtree.getLeafZones.asScala.map(_.partitionId)
    non_empties.filter(id => ids.contains(id))
  }

  // Test if the cell is in the non-empty list...
  private def isInNonEmpties(cell: Cell, non_empties: List[Int]): Boolean =
    non_empties.exists(_ == cell.id)

  def main(args: Array[String]) = {
    implicit val geofactory = new GeometryFactory(new PrecisionModel(100))
    implicit val settings = Settings(tolerance = 0.001, debug = true)
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
        val cell = Cell(id, leaf.lineage, leaf.getEnvelope)
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
    val empties = (0 to 51).toSet.diff(non_empties.toSet).toList.sorted

    val L = for{
      empty <- empties
    } yield {
      val c = cells(empty)
      val cell = Cell(c.id, c.lineage, c.mbr.getEnvelopeInternal)
      getClosestPath(quadtree, non_empties, cell)
    }

    val result = L.groupBy{ case(pid, point, pids) => (pid, point) }
      .map{ case(key, value) =>
        val pid = key._1
        val point = key._2
        val pids = value.map(_._3).flatten.toSet

        pids.map( p => (point, pid, p))
      }.flatten

    save("/tmp/edgesR.wkt"){
      result.map{ case(point, pid, p) =>
        val wkt = cells(p).wkt
        s"$wkt\t$p\t$pid\n"
      }.toList
    }

    spark.close()
  }

}

