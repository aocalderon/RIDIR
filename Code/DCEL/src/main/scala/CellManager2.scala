import DCELMerger.{geofactory, precision, logger}
import DCELMerger.{envelope2polygon, save}
import org.datasyslab.geospark.spatialPartitioning.quadtree.{StandardQuadTree, QuadRectangle}
import org.apache.spark.rdd.RDD
import com.vividsolutions.jts.geom.{Envelope, Coordinate, Point, LineString, Polygon}
import com.vividsolutions.jts.io.WKTReader
import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap
import scala.collection.mutable.{HashSet, ListBuffer}
import scala.annotation.tailrec

object CellManager2{
  // Model the cell of a quadtree...
  case class Cell(id: Int, lineage: String, boundary: Envelope)
  // Model the relation between an 'empty' cell and its closest 'non-empty' cell...
  case class ClosestCell(lineage: String, closest: String, point: Point)

  // Identify if a cell is 'empty'...
  // 'empty' means a face match the boundary of its cell and do not have a valid Id.
  def isFaceEqualBoundary(face: Face, boundary: Polygon): Boolean = {
    if(face.id.substring(0,1) != "F"){
      false
    } else if(face.getNVertices != 5){
      false
    } else if(face.isCW) {
      false
    } else {
      val reader = new WKTReader(geofactory)
      val a = reader.read(face.toPolygon().toText())
      val b = reader.read(boundary.toText())
      a.equals(b)
    }
  }

  // Return a map of the 'empty' cells...
  def getEmptyCells(dcelRDD: RDD[LDCEL],
    quadtree: StandardQuadTree[LineString]): HashMap[String, Cell] = {

    val cells = quadtree.getLeafZones.asScala.map{ leaf =>
      val id = leaf.partitionId.toInt
      val lineage = leaf.lineage
      val boundary = leaf.getEnvelope
      id -> Cell(id, lineage, boundary)
    }.toMap

    val M = dcelRDD.mapPartitionsWithIndex{ case(index, iter) =>
      val dcel = iter.next()
      val cell = cells(index)
      val boundary = envelope2polygon(cell.boundary)
      val face = dcel.faces.filter(f => isFaceEqualBoundary(f, boundary))

      if(!face.isEmpty){
        face.head.isCellFace = true
        Iterator(cell)
      } else {
        List.empty[Cell].toIterator
      }
      
    }.collect().map{ cell =>
      cell.lineage -> cell
    }

    HashMap(M:_*)
  }

  // Divide the quadtree's cells if they are 'empty' or not...
  def divideEmptyAndNonEmptyCells(dcelRDD: RDD[LDCEL],
    quadtree: StandardQuadTree[LineString]):
      (HashMap[String, Cell], HashMap[String, Cell]) = {

    val cells = quadtree.getLeafZones.asScala.map{ leaf =>
      val id = leaf.partitionId.toInt
      val lineage = leaf.lineage
      val boundary = leaf.getEnvelope
      id -> Cell(id, lineage, boundary)
    }.toMap

    val M = dcelRDD.mapPartitionsWithIndex{ case(index, iter) =>
      val dcel = iter.next()
      val cell = cells(index)
      val boundary = envelope2polygon(cell.boundary)
      val face = dcel.faces.filter(f => isFaceEqualBoundary(f, boundary))

      if(!face.isEmpty){
        face.head.isCellFace = true
      }

      val tuple = (cell.lineage -> cell, face.isEmpty)
      Iterator(tuple)
      
    }.collect()
    val (empties, nonempties) = M.partition(_._2)

    (HashMap(empties.map(_._1):_*), HashMap(nonempties.map(_._1):_*))
  }

  def groupByLineage(emptyCells: Vector[Cell]): (Vector[Cell], Vector[Cell])= {
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
        val boundary = children._3.map(_.boundary)
          .reduce{ (a, b) =>
            a.expandToInclude(b)
            a
          }
        Cell(-1, lineage, boundary)
      }

    (merge_them, keep_them)
  }

  @tailrec
  def mergeEmptyCells(empties: Map[Int, Vector[Cell]],
    levels: List[Int],
    previous: Vector[Cell],
    accum: Vector[Cell]): Vector[Cell] = {

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
    empties: HashMap[String, Cell],
    nonempties: HashMap[String, Cell]):
      (edu.ucr.dblab.StandardQuadTree[T], HashMap[String, Cell]) = {

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

  def cleanQuadtreeTester[T](original_quadtree: edu.ucr.dblab.StandardQuadTree[T],
    empties: HashMap[String, Cell],
    nonempties: HashMap[String, Cell]):
      (edu.ucr.dblab.StandardQuadTree[T], HashMap[String, Cell]) = {

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

  
  def getClosestCell(
    quadtree: edu.ucr.dblab.StandardQuadTree[LineString],
    empties: HashMap[String, Cell],
    debug: Boolean = false): Vector[ClosestCell] = {

    var closestCellList = new ListBuffer[ClosestCell]()
    var R = new HashSet[String]()

    val emptyLineages = empties.keySet

    if(debug){
      empties.foreach{println}
      println(s"Empty Cells size: ${empties.size}")
      println(s"keys: ${emptyLineages.mkString(" ")}")
    }

    val n = ""
    //val n = "10"
    empties.keys.map{ lineage =>
      if(R.contains(lineage)){
      } else {
        var cellList = new ListBuffer[Cell]()
        var closestCell = ""
        var referenceCorner = geofactory.createPoint(new Coordinate(0,0))
        cellList += empties(lineage)
        var done = false
        while(!done){

          cellList = cellList.distinct
          val c = cellList.last

          if(debug){
            val list = cellList.map(_.lineage).mkString(" ")
            println(s"Current in the list: $list")
          }

          val (ncells, corner) = getCellsAtCorner(quadtree, c)

          if(debug){
            val a = c.lineage
            val b = ncells.map(_.lineage).mkString(" ")
            val p = corner.toText
            println(s"$a => IDs: $b [$p]")
          }

          val ncells_prime = ncells.filter{ ncell =>
            !emptyLineages.contains(ncell.lineage)
          }

          if(debug){
            ncells_prime.map(_.lineage).foreach(println)
          }

          done = ncells_prime match {
            case Nil => false
            case nonempties  => {
              closestCell = nonempties.head.lineage
              referenceCorner = corner
              true
            }
          }

          if(!done){
            cellList ++= ncells
          }

          if(debug){
            if(lineage == n)
              System.exit(0)
          }
          
        }
        for(cell <- cellList){
          R += cell.lineage
          closestCellList += ClosestCell(cell.lineage, closestCell, referenceCorner)
        }
      }
    }
    closestCellList.toVector
  }

  def getCellsAtCorner(quadtree: edu.ucr.dblab.StandardQuadTree[LineString],
    c: Cell): (List[Cell], Point) = {

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
    envelope.expandBy(precision)
    val cells = quadtree.findZones(new edu.ucr.dblab.QuadRectangle(envelope)).asScala
      .filterNot(_.lineage == c.lineage)
      .map{ q =>
        val id = q.partitionId.toInt
        Cell(id, q.lineage, q.getEnvelope)
      }.sortBy(_.lineage.size).toList
    (cells, corner)
  }

  def updateCellsWithoutId(dcelRDD: RDD[LDCEL],
    original_quadtree: StandardQuadTree[LineString],
    label: String = "A",
    debug: Boolean = false): RDD[LDCEL] = {

    val leafs = original_quadtree.getLeafZones.asScala.toList
    val lineages = leafs.map(_.lineage).sorted
    val boundary = original_quadtree.getZone.getEnvelope
    val quad = Quadtree.create[LineString](boundary, lineages)
    val (empties, nonempties) = divideEmptyAndNonEmptyCells(dcelRDD, original_quadtree)

    val (quadtree_prime, empties_prime) = cleanQuadtree(
      original_quadtree,
      empties,
      nonempties)

    if(debug){
      save{s"/tmp/empties${label}.tsv"}{
        empties.values.map(m => s"${m.lineage}\n").toSeq
      }
      save{s"/tmp/quadtree.tsv"}{
        original_quadtree.getLeafZones.asScala.map{ leaf =>
          s"${leaf.lineage}\t${leaf.partitionId}\n"
        }.toList.sorted
      }
      save{s"/tmp/boundary.tsv"}{
        val boundary = envelope2polygon(original_quadtree.getZone.getEnvelope).toText
        List(s"$boundary\n")
      }
    }

    logger.info("getClosestCell...")
    val closestList = getClosestCell(quadtree_prime, empties_prime)
    logger.info("getClosestCell... Done!")

    closestList.foreach{println}

    /*
    val fcells = dcelRDD.mapPartitionsWithIndex{ case(index, iter) =>
      val dcel = iter.next()
      val r = if(ecells.map(_._2).contains(index)){
        val ecs = ecells.filter(_._2 == index).map{ e =>
          val id = e._1
          val corner = e._3.getEnvelopeInternal
          corner.expandBy(precision)
          (id, envelope2polygon(corner))
        }
        val fcs = dcel.faces.map{ f =>
          val id = f.id
          val face = f.getGeometry._1
          (id, face)
        }
        
        for{
          ecell <- ecs
          // Query which face it intersects...
          fcell <- fcs if fcell._2.intersects(ecell._2)
        } yield {
          (ecell._1, fcell._1)
        }
      } else {
        Vector.empty[(Int, String)]
      }
      r.toIterator
    }.collect().filter(_._2.substring(0, 1) != "F")
     */

    /*
    val dcelRDD_prime = dcelRDD.mapPartitionsWithIndex{ case(index, iter) =>
      val dcel = iter.next()
      if(fcells.map(_._1).contains(index)){
        val cell = fcells.filter(_._1 == index)
        val id = cell.head._2
        
        dcel.faces.filter(_.isCellFace).map{ f =>
          f.id = id
          f.getHedges.foreach(_.id = id)
          f
        }
        val d = LDCEL(index, dcel.vertices, dcel.half_edges, dcel.faces, dcel.index)

        Iterator(d)
      } else {
        // prune empty faces...
        val faces = dcel.faces.filter(_.id.substring(0, 1) != "F")
        val d = LDCEL(index, dcel.vertices, dcel.half_edges, faces, dcel.index)
        Iterator(d)
      }
    }
     */

    dcelRDD
  } 
}
