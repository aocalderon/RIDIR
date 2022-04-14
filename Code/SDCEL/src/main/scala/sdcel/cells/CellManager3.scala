package edu.ucr.dblab.sdcel.cells

import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.geom.{Geometry, Envelope, Coordinate}
import com.vividsolutions.jts.geom.{Point, LineString, Polygon}

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

import scala.collection.immutable.HashMap
import scala.collection.JavaConverters._
import scala.annotation.tailrec

//import org.slf4j.{Logger, LoggerFactory}

import edu.ucr.dblab.sdcel.PartitionReader._
import edu.ucr.dblab.sdcel.Utils.{Settings, save, getPartitionLocation, logger, log, log2}
import edu.ucr.dblab.sdcel.quadtree.{StandardQuadTree, QuadRectangle, Quadtree}
import edu.ucr.dblab.sdcel.geometries.{Cell, Half_edge, EdgeData}
import edu.ucr.dblab.sdcel.Params

object EmptyCellManager2 {

  def getEmptyCells(data: RDD[LineString], letter: String = "A")
    (implicit settings: Settings, geofactory: GeometryFactory,
      cells: Map[Int, Cell]): Array[(Int, Boolean)] = {

    val empties_prime = data.mapPartitionsWithIndex{ (pid, it) =>
      val cell = cells(pid)
      val cenvelope = cell.boundary
      val non_empty = it.filter{ edge =>
        edge.getEnvelopeInternal.intersects(cenvelope)
      }.exists{ edge =>
        edge.getUserData.asInstanceOf[EdgeData].crossingInfo != "None"
      }
      val r = (cell.id, non_empty)
      Iterator(r)
    }//.persist(settings.persistance)
    val empties = empties_prime.collect()

    if(settings.debug){
      log(s"INFO|nEdges${letter}=${data.count}")
      log(s"INFO|nEmpties${letter}=${empties.filter(!_._2).size}")
      save(s"/tmp/edgesE${letter}.wkt"){
        empties.map{ e =>
          val cell = cells(e._1)
          val wkt = cell.wkt
          val emp = e._2

          s"$wkt\t$emp\n"
        }
      }
    }

    empties
  }

  case class EmptyCell(point: Point, pid: Cell, empty:Cell,
    polyId: Int = -1, label: String = "*"){

    // return a expanded version of the point to help in the intersection call...
    def reference(implicit settings: Settings, geofactory: GeometryFactory): Polygon = {
      val envelope = point.getEnvelopeInternal
      envelope.expandBy(settings.tolerance)
      envelope2polygon(envelope)
    }

    def getLids: List[String] = if(empty.lids.isEmpty) List(empty.lineage) else empty.lids 

    override def toString: String = { s"$point\t" +
      s"L${pid.lineage}\tL${empty.lineage}\t" +
      s"${polyId}\t${label}\t"
    }
  }

  case class ECellMap(lid1: String, lid2: String)
  case class ECellInfo(lid2: String, empty: EmptyCell)

  def runEmptyCells[T](sdcel: RDD[(Half_edge, String, Envelope, Polygon)],
    empties: Array[(Int, Boolean)], letter: String = "A")
    (implicit geofactory: GeometryFactory, settings: Settings, spark: SparkSession,
      quadtree: StandardQuadTree[T], cells: Map[Int, Cell]): Map[String, EmptyCell] = {

    val (ne, e) = empties.map{ case(id, emp) => (cells(id), emp) }
      .partition{ case(cell, non_empty) => non_empty }

    if(e.isEmpty){
      Map.empty[String, EmptyCell]
    } else {
      val non_empties = ne.map(_._1).toList
      val empties = e.map(_._1).toList

      val empties_prime  =     empties.map(cell => (cell.lineage, cell)).toList
      val emptiesHash    = HashMap( empties_prime.map( i => i._1 -> i._2): _* )
      val nempties_prime = non_empties.map(cell => (cell.lineage, cell)).toList
      val nemptiesHash   = HashMap(nempties_prime.map( i => i._1 -> i._2): _* )

      val (quadtree_prime, empties3) = cleanQuadtree(quadtree, emptiesHash, nemptiesHash)

      val cells_prime = quadtree_prime.getLeafZones.asScala.map{ leaf =>
        val ring = envelope2ring(leaf.getEnvelope)
        val id = leaf.partitionId
        val lineage = leaf.lineage
        id.toInt -> Cell(id, lineage, ring)
      }.toMap

      if(settings.debug){
        save(s"/tmp/edgesP${letter}.wkt"){
          cells_prime.values.map{ cell =>
            val wkt = cell.wkt
            s"$wkt\n"
          }.toList
        }
      }

      val r_prime = solve(quadtree_prime, cells_prime, non_empties, empties3.values.toList)

      if(settings.debug){
        save(s"/tmp/edgesR${letter}.wkt"){
          r_prime.map{ r =>
            val wkt   = r.point
            val cid1  = r.pid.id
            val lid1  = r.pid.lineage 
            val lid2  = r.empty.lineage
            val lids  = r.getLids.mkString(" ")
            val nlids = r.getLids.size
            s"$wkt\t$cid1\tL$lid1\tL$lid2\t$nlids\t$lids\n"
          }.toList
        }
      }

      // Getting polygon IDs from known partitions...
      val r = updatePolygonIds(sdcel, r_prime, cells)
      val as = empties3.values.flatMap(e => e.getLids.map(l => ECellMap(l , e.lineage)))
      val bs = r.map(ec => ECellInfo(ec.empty.lineage, ec))

      val cells_map = (for {
        a <- as
        b <- bs if a.lid2 == b.lid2
      } yield {
        a.lid1 -> b.empty
      }).toMap

      if(settings.debug){
        save(s"/tmp/edgesM${letter}.wkt"){
          cells_map.map{ case(k, v) =>
            val wkt   = v.point.toText
            val lid1  = k
            val lid2  = v.empty.lineage
            val label = v.label
            s"$wkt\tL$lid1\tL$lid2\t$label\n"
          }.toList
        }
        save(s"/tmp/edgesF${letter}.wkt"){
          sdcel.mapPartitionsWithIndex{ (pid, it) =>
            getFaces(it, cells(pid), cells_map)
              .map{ hedge =>
                s"${hedge._4.toText}\t${hedge._2}\t${pid}\n"
              }.toIterator
          }.collect
        }
      }

      cells_map
    }
  }

  def getFaces(it: Iterator[(Half_edge, String, Envelope, Polygon)], cell: Cell,
    m: Map[String, EmptyCell])(implicit geofactory: GeometryFactory)
      : List[(Half_edge, String, Envelope, Polygon)] = {

    val lid = cell.lineage

    if(m.keySet.contains(lid)){
      val empty = m(lid)
      val h = cell.toHalf_edge(empty.polyId, empty.label.substring(0, 1))
      
      val poly = cell.toPolygon
      val tuple = (h, empty.label, poly.getEnvelopeInternal, poly)
      it.toList :+ tuple
    } else {
      it.toList
    }
  }
    
  def getFullSetHedges(it: Iterator[(Half_edge, String, Envelope, Polygon)])
      : List[(Half_edge, String, Envelope, Polygon)] = {

    it.flatMap{ case(hedge,l,e,p) =>
      hedge.getNexts.map{h => (h,l,e,p)}
    }.toList
  }

  def updatePolygonIds(sdcel: RDD[(Half_edge, String, Envelope, Polygon)],
    r: List[EmptyCell], cells: Map[Int, Cell])
    (implicit settings: Settings, geofactory: GeometryFactory)
      : List[EmptyCell] = {

    
    val lids = r.map(r => r.pid.lineage).toSet
    sdcel.mapPartitionsWithIndex{ (pid, it) =>
      val lid = cells(pid).lineage
      val result = if(lids.contains(lid)){
        
        val points  = r.filter(_.pid.lineage == lid)

        val faces = it.map{ f => (f._2, f._3, f._4, f._1) }

        val faces_prime = for {
          point <- points
          face <- faces if face._2.intersects(point.reference.getEnvelopeInternal)
        } yield {
          face
        }

        val list = for{
          point  <- points
          face <- faces_prime if face._3.intersects(point.reference)
        } yield {
          val label = face._1
          val polyId = try{
            label.substring(1).toInt
          } catch {
            case e: java.lang.NumberFormatException => {
              val ploc = getPartitionLocation(pid)
              logger.info(s"ERROR|${face._4.wkt}\t$label\t$pid\t$ploc")
              -1
            }
          }
          point.copy(polyId = polyId, label = label)
        }

        list.toIterator
      } else {
        List.empty[EmptyCell].toIterator
      }
      result
    }.collect.toList
  }

  def solve[T](quadtree: StandardQuadTree[T], cells: Map[Int, Cell],
    non_empties: List[Cell], empties: List[Cell])
    (implicit geofactory: GeometryFactory, settings: Settings)
      : List[EmptyCell] = {

    val s = solveRec(quadtree, cells, empties, non_empties, List.empty[(Point,Cell,Cell)])
    s.map{ case(point, pid, empty) =>
      (empty.lineage) -> EmptyCell(point, pid, empty)
    }.toMap.values.toList
  }

  @tailrec
  private def solveRec[T](quadtree: StandardQuadTree[T], cells: Map[Int, Cell],
    empties: List[Cell], non_empties: List[Cell], result: List[(Point, Cell, Cell)])
    (implicit geofactory: GeometryFactory, settings: Settings)
      : List[(Point, Cell, Cell)] = {

    // iterate recursively over the set of empty cells...
    empties match {
      case Nil => result
      case empty +: tail => {
        val cell = empty
        val (pid, point, pids) = getClosestPath(quadtree, non_empties, cell)
        // remove the complete path of cells solved in the past call...
        val new_empties = tail.toSet.diff(pids.toSet).toList
        val new_result = result ++ pids.map( p => (point, pid, p))
        solveRec(quadtree, cells, new_empties, non_empties, new_result)
      }
    }
  }

  // helper function to call closest function
  private def getClosestPath[T](Q: StandardQuadTree[T], NE: List[Cell], c: Cell)
    (implicit geofactory: GeometryFactory, settings: Settings):
      (Cell, Point, List[Cell]) = {

    // just create a fake point and empty list to start the recursion...
    val point_prime = geofactory.createPoint(new Coordinate(0,0))
    val path_prime  = List.empty[Cell]
    val (pid, path, point) = closest(Q, NE, c, path_prime, point_prime)

    (pid, point, path :+ c) // attach the current cell to the path
  }

  @tailrec
  // Recursive function to get the list of empty cells and the non-empty cell and point
  // which is the closest to them...
  private def closest[T](Q: StandardQuadTree[T], NE: List[Cell], c: Cell,
    path: List[Cell], point: Point)
    (implicit geofactory: GeometryFactory, settings: Settings):
      (Cell, List[Cell], Point) = {

    // get cells in the corner and reference point...
    val (cs, point) = getCellsAtCorner(Q, c)

    // if one of the cells is non-empty we can finish...
    if(cs.exists(x => isInNonEmpties(x, NE))){
      val (non_empties, empties) = cs.partition(x => isInNonEmpties(x, NE))
      val non_empty = non_empties.head
      
      (non_empty, path ++ empties, point)
    } else { // if not...
      val new_c = getDeepestCell(cs) // choose the deepest cell...
      // extract the cell parent quadtree...  
      val Q_prime = Quadtree.extract(Q, getLineageParent(new_c))
      // filter the non-empty cells to only the children of the choosen cell parent...
      val new_NE = filterNonEmpties(NE, Q_prime) 
      val new_path = path ++ cs // accumulate the set of empty cells involved...
      // repeat recursion...
      closest(Q, new_NE, new_c, new_path, point)
    }
  }

  // Return the 3 cells that touch the internal corner of the given cell...
  // The internal corner is that one which point to the interior
  // of the cell parent...
  private def getCellsAtCorner[T](quadtree: StandardQuadTree[T], c: Cell)
    (implicit geofactory: GeometryFactory, settings: Settings)
      : (List[Cell], Point) = {

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
    envelope.expandBy(settings.tolerance + 0.001) // expand corner a bit to ensure touch...

    // from quadtree get the cells that touch the corner...
    val cells = quadtree.findZones(new QuadRectangle(envelope)).asScala
      .filterNot(_.lineage == c.lineage) // remove current cell...
      .map{ q =>
        val id = q.partitionId.toInt
        Cell(id, q.lineage, envelope2ring(q.getEnvelope))
      }.toList

    if(cells.size < 3){
      logger.info(s"Error at getCellsAtCorner (#cells=${cells.size})")
      logger.info(s"Point = ${corner.toText()}")
      logger.info(s"Cell  = ${c.wkt}")
    }

    (cells, corner)
  }

  // Return the lineage of this cell parent...
  private def getLineageParent(cell: Cell): String =
    cell.lineage.substring(0, cell.lineage.length - 1)

  // Return the cell with the deepest lineage in the list...
  private def getDeepestCell(cells: List[Cell]): Cell = 
    cells.map(cell => (cell.lineage.length, cell)).maxBy(_._1)._2

  // Filter the list of non-empty cells with those from the new quadtree...
  private def filterNonEmpties[T](non_empties: List[Cell],
    quadtree: StandardQuadTree[T]): List[Cell] = {

    val ids = quadtree.getLeafZones.asScala.map(_.lineage)
    non_empties.filter(cell => ids.contains(cell.lineage))
  }

  // Test if the cell is in the non-empty list...
  private def isInNonEmpties(cell: Cell, non_empties: List[Cell]): Boolean =
    non_empties.exists(c => c.lineage == cell.lineage)

  def groupByLineage(emptyCells: Vector[Cell])(implicit geofactory: GeometryFactory)
      : (Vector[Cell], Vector[Cell])= {
    val (a, b) = emptyCells
      .map{ cell =>
        val lineage = cell.lineage
        val parent_lineage = lineage.reverse.tail.reverse
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
      val lids = children._3.filter(_.id != -1).map{_.lineage}
      val boundary = children._3.map(_.toPolygon.getEnvelopeInternal)
        .reduce{ (a, b) =>
          a.expandToInclude(b)
          a
        }
      val c = Cell(-1, lineage, envelope2ring(boundary))
      c.lids = children._3.flatMap{_.lids} ++ lids
      c
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

    val emptiesByLevel = empties.values.map{ cell => (cell.lineage.size, cell) }
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
}

