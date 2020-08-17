import org.datasyslab.geospark.spatialPartitioning.quadtree.{StandardQuadTree, QuadRectangle}
import org.apache.spark.rdd.RDD
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.geom.{Envelope, Coordinate, Point, LineString, Polygon}
import com.vividsolutions.jts.io.WKTReader
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._
import DCELMerger.{timer, geofactory, precision, logger}
import DCELBuilder.save
import scala.annotation.tailrec

case class Cell(id: Int, lineage: String, envelope: Envelope, ids: List[Int] = List.empty[Int])

object CellManager{

  def envelope2Polygon(e: Envelope): Polygon = {
    val minX = e.getMinX()
    val minY = e.getMinY()
    val maxX = e.getMaxX()
    val maxY = e.getMaxY()
    val p1 = new Coordinate(minX, minY)
    val p2 = new Coordinate(maxX, minY)
    val p3 = new Coordinate(maxX, maxY)
    val p4 = new Coordinate(minX, maxY)
    geofactory.createPolygon(Array(p1,p2,p3,p4,p1))
  }

  def equalFaceAndCell(face: Face, cell: Polygon): Boolean = {
    if(face.id.substring(0,1) != "F"){
      false
    } else if(face.getNVertices != 5){
      false
    } else if(face.isCW) {
      false
    } else {
      val reader = new WKTReader(geofactory)
      val a = reader.read(face.toPolygon().toText())
      val b = reader.read(cell.toText())
      a.equals(b)
    }
  }

  def getEmptyCells(dcelRDD: RDD[LDCEL], grids: Map[Int, QuadRectangle]): Map[Int, Int] = {
    val M = dcelRDD.mapPartitionsWithIndex{ case(index, iter) =>
      val dcel = iter.next()
      val index = dcel.index
      val cell = envelope2Polygon(grids(index).getEnvelope)
      val face = dcel.faces.filter(f => equalFaceAndCell(f, cell))
      val m = if(!face.isEmpty){
        face.head.isCellFace = true
        (index, 0)
      } else {
        (index, 1)
      }

      Iterator(m)
    }

    M.collect().toMap
  }

  def getCellsAtCorner(quadtree: StandardQuadTree[LineString], c: Cell): (List[Cell], Point) = {
    val region = c.lineage.takeRight(1).toInt
    val corner = region match {
      case 0 => geofactory.createPoint(new Coordinate(c.envelope.getMaxX, c.envelope.getMinY))
      case 1 => geofactory.createPoint(new Coordinate(c.envelope.getMinX, c.envelope.getMinY))
      case 2 => geofactory.createPoint(new Coordinate(c.envelope.getMaxX, c.envelope.getMaxY))
      case 3 => geofactory.createPoint(new Coordinate(c.envelope.getMinX, c.envelope.getMaxY))
    }
    val envelope = corner.getEnvelopeInternal
    envelope.expandBy(precision)
    val cells = quadtree.findZones(new QuadRectangle(envelope)).asScala
      .filterNot(_.partitionId == c.id)
      .map(q => Cell(q.partitionId.toInt, q.lineage, q.getEnvelope))
      .sortBy(_.lineage.size).toList
    (cells, corner)
  }

  def getNextCellWithEdges(M: Map[Int, Int], quadtree: StandardQuadTree[LineString], grids: Map[Int, QuadRectangle], label: String = "A"): List[(Int, Int, Point)] = {
    val cells = grids.map(grid => grid._1 -> Cell(grid._2.partitionId.toInt, grid._2.lineage, grid._2.getEnvelope)).toMap
    var result = new ListBuffer[(Int, Int, Point)]()
    var R = new ListBuffer[Int]()

    //
    println(s"M: ${M.size}...")
    save{s"/tmp/M${label}.tsv"}{
      M.toList.map{ case (a, b) =>
        s"${a}\t${b}\n"
      }
    }

    val Z = M.filter{ case(index, size) => size == 0 }.map(_._1).toVector.sorted

    //
    println(s"Z: ${Z.size}...")

    Z.map{ index =>
      if(R.contains(index)){
      } else {
        var cellList = new ListBuffer[Cell]()
        var nextCellWithEdges = -1
        var referenceCorner = geofactory.createPoint(new Coordinate(0,0))
        cellList += cells(index)
        var done = false
        while(!done){
          val c = cellList.last
          val (ncells, corner) = getCellsAtCorner(quadtree, c)

          //
          //println(s"$c => IDs: ${ncells.map(_.id).mkString(" ")} Corner: ${corner.toText()}")

          val ncells_prime = ncells.par.map(c => (c, M(c.id))).filter(_._2 != 0)

          //
          //ncells_prime.foreach(println)

          val hasEdges = if(ncells_prime.size == 0) None else Some(ncells_prime.head._1)

          //
          //println(hasEdges)

          done = hasEdges match {
            case Some(cell) => {
              nextCellWithEdges = cell.id
              referenceCorner = corner
              true
            }
            case None => false
          }

          if(!done){
            cellList ++= ncells
          }
          if(!done && cellList.takeRight(4).map(_.lineage.size).distinct.size == 1){
            val quads = cellList.takeRight(4).sortBy(_.lineage)
            val parent_pos = quads.head.lineage.takeRight(2).head
            val new_c_pos = parent_pos match {
              case '0' => '3'
              case '1' => '2'
              case '2' => '1'
              case '3' => '0'
              case _ => ' '
            }
            cellList.remove(cellList.size - 4, 4)
            cellList = cellList ++ quads.filter(_.lineage.last != new_c_pos)
            val c_prime = quads.filter(_.lineage.last == new_c_pos).head
            val l_prime = c_prime.lineage
            cellList += c_prime.copy(lineage = l_prime.substring(0, l_prime.size - 1))
          }
        }
        for(cell <- cellList){
          R += cell.id
          val r = (cell.id, nextCellWithEdges, referenceCorner)
          result += r
        }
        R = R.sorted
      }
    }
    result.toList
  }

  ///////////
  import scala.collection.mutable.HashSet

  def groupByLineage(emptyCells: Vector[edu.ucr.dblab.QuadRectangle]):
      (Vector[edu.ucr.dblab.QuadRectangle], Vector[edu.ucr.dblab.QuadRectangle])= {
    val E = emptyCells
      .map{ cell =>
        val parent_lineage = cell.lineage.reverse.tail.reverse
        (parent_lineage, cell)
      }
      .groupBy(_._1)
      .map{ case(parent, cells) =>
        val children = cells.map(_._2).toList

        (parent, children.length, children)
      }

    val K = E.filter(_._2 != 4)
      .flatMap(_._3)
      .toVector

    val M = E.filter(_._2 == 4)
      .toVector
      .map{ children =>
        val lineage = children._1
        val envelope = children._3.map(_.getEnvelope)
          .reduce{ (a, b) =>
            a.expandToInclude(b)
            a
          }
        val rectangle = new edu.ucr.dblab.QuadRectangle(envelope)
        rectangle.lineage = lineage
        rectangle
      }

    (M, K)
  }

  @tailrec
  def mergeEmptyCells(empties: Map[Int, Vector[edu.ucr.dblab.QuadRectangle]],
    levels: List[Int],
    previous: Vector[edu.ucr.dblab.QuadRectangle],
    accum: Vector[edu.ucr.dblab.QuadRectangle]): Vector[edu.ucr.dblab.QuadRectangle] = {

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

  def cleanQuadtree[T](quadtree: edu.ucr.dblab.StandardQuadTree[T],
    M: Map[Int, Int]): edu.ucr.dblab.StandardQuadTree[T] = {

    val X = M.filter(_._2 == 0).map(_._1).toSet
    val emptyCells = quadtree.getLeafZones.asScala
      .filter(leaf => X.contains(leaf.partitionId))

    val empties = emptyCells.map(cell => (cell.lineage.length(), cell))
      .groupBy(_._1)
      .map(g => g._1 -> g._2.map(_._2).toVector)
      .withDefaultValue(Vector.empty[edu.ucr.dblab.QuadRectangle])

    val levels = (1 to empties.keys.max).toList.reverse
    
    val previous = Vector.empty[edu.ucr.dblab.QuadRectangle]
    val accum = Vector.empty[edu.ucr.dblab.QuadRectangle]

    val emptyCells_prime = mergeEmptyCells(empties, levels, previous, accum)

    val Y = M.filter(_._2 == 1).map(_._1).toSet
    val nonemptyCells = quadtree.getLeafZones.asScala
      .filter(leaf => Y.contains(leaf.partitionId))

    val boundary = quadtree.getZone().getEnvelope()
    val lineages = (nonemptyCells ++ emptyCells_prime).map(_.lineage).toList

    Quadtree.create[T](boundary, lineages) 
  }

  def getNextCellWithEdges2(M: Map[Int, Int], quadtree: edu.ucr.dblab.StandardQuadTree[LineString]): List[(Int, Int, Point)] = {
    val cells = quadtree.getLeafZones.asScala.map{ cell =>
      val id = cell.partitionId.toInt
      id -> Cell(id, cell.lineage, cell.getEnvelope, List(id))
    }.toMap
    var result = new ListBuffer[(Int, Int, Point)]()
    var R = new HashSet[Int]()

    val X = M.filter(_._2 == 1).map(_._1).toVector.sorted
    //val Y = M.filter(_._2 == 0).map(_._1).toSet

    //
    logger.info(s"M: ${M.size}")
    logger.info(s"X: ${X.size}")
    //

    //val n = Int.MaxValue
    val n = 1583
    X.map{ index =>
      if(R.contains(index)){
      } else {
        var cellList = new ListBuffer[Cell]()
        var nextCellWithEdges = -1
        var referenceCorner = geofactory.createPoint(new Coordinate(0,0))
        cellList += cells(index)
        var done = false
        while(!done){

          cellList = cellList.distinct
          val c = cellList.last

          //
          println(cellList.map(_.id).mkString(" "))
          //

          val (ncells, corner) = getCellsAtCorner2(quadtree, c)

          //
          println(s"${c.id} => IDs: ${ncells.map(_.id).mkString(" ")} Corner: ${corner.toText()}")
          //

          val ncells_prime = ncells.map(c => (c, M(c.id))).filter(_._2 != 0)

          //
          ncells_prime.foreach(println)
          //

          val hasEdges = if(ncells_prime.size == 0) None else Some(ncells_prime.head._1)

          //
          println(hasEdges)
          //

          done = hasEdges match {
            case Some(cell) => {
              nextCellWithEdges = cell.id
              referenceCorner = corner
              true
            }
            case None => false
          }

          if(!done){
            cellList ++= ncells
          }

          //
          logger.info(s"$index")
          if(index >= n)
            System.exit(0)
          //
          
        }
        for(cell <- cellList){
          R = R ++ cell.ids
          for(id <- cell.ids){
            val r = (id, nextCellWithEdges, referenceCorner)
            result += r
          }
        }
      }
    }
    result.toList
  }
  def getCellsAtCorner2(quadtree: edu.ucr.dblab.StandardQuadTree[LineString], c: Cell): (List[Cell], Point) = {
    val region = c.lineage.takeRight(1).toInt
    val corner = region match {
      case 0 => geofactory.createPoint(new Coordinate(c.envelope.getMaxX, c.envelope.getMinY))
      case 1 => geofactory.createPoint(new Coordinate(c.envelope.getMinX, c.envelope.getMinY))
      case 2 => geofactory.createPoint(new Coordinate(c.envelope.getMaxX, c.envelope.getMaxY))
      case 3 => geofactory.createPoint(new Coordinate(c.envelope.getMinX, c.envelope.getMaxY))
    }
    val envelope = corner.getEnvelopeInternal
    envelope.expandBy(precision)
    val cells = quadtree.findZones(new edu.ucr.dblab.QuadRectangle(envelope)).asScala
      .filterNot(_.partitionId == c.id)
      .map{ q =>
        val id = q.partitionId.toInt
        Cell(id, q.lineage, q.getEnvelope, List(id))
      }.sortBy(_.lineage.size).toList
    (cells, corner)
  }
  def updateCellsWithoutId2(dcelRDD: RDD[LDCEL],
    previous_quadtree: StandardQuadTree[LineString],
    label: String = "A",
    debug: Boolean = false): RDD[LDCEL] = {

    val leafs = previous_quadtree.getLeafZones.asScala.toList
    val grids = leafs.map(leaf => leaf.partitionId.toInt -> leaf).toMap
    val lineages = leafs.map(_.lineage).sorted
    val boundary = previous_quadtree.getZone.getEnvelope
    val quad = Quadtree.create[LineString](boundary, lineages)
    val M = getEmptyCells(dcelRDD, grids)
    val quadtree = cleanQuadtree(quad, M)

    if(debug){
      save{s"/tmp/M${label}.tsv"}{
        M.map(m => s"${m._1}\t${m._2}\n").toSeq
      }
      save{s"/tmp/quadtree.tsv"}{
        previous_quadtree.getLeafZones.asScala.map{ leaf =>
          s"${leaf.lineage}\t${leaf.partitionId}\n"
        }.toList.sorted
      }
      save{s"/tmp/boundary.tsv"}{
        val boundary = envelope2Polygon(quadtree.getZone.getEnvelope).toText
        List(s"$boundary\n")
      }
    }

    logger.info("getNextCellWithEdges...")
    val ecells = getNextCellWithEdges2(M, quadtree)
    logger.info("getNextCellWithEdges... Done!")

    val fcells = dcelRDD.mapPartitionsWithIndex{ case(index, iter) =>
      val dcel = iter.next()
      val r = if(ecells.map(_._2).contains(index)){
        val ecs = ecells.filter(_._2 == index).map{ e =>
          val id = e._1
          val corner = e._3.getEnvelopeInternal
          corner.expandBy(precision)
          (id, envelope2Polygon(corner))
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

    dcelRDD_prime
  }
  ////////////////

  def updateCellsWithoutId(dcelRDD: RDD[LDCEL], quadtree: StandardQuadTree[LineString], grids: Map[Int, QuadRectangle], label: String = "A"): RDD[LDCEL] = {

    logger.info("getEmptyCells...")
    val M = getEmptyCells(dcelRDD: RDD[LDCEL], grids: Map[Int, QuadRectangle])
    logger.info("getEmptyCells... Done")

    logger.info("getNextCellWithEdges...")

    val ecells = getNextCellWithEdges(M, quadtree, grids, label)
    logger.info("getNextCellWithEdges... Done!")

    val fcells = dcelRDD.mapPartitionsWithIndex{ case(index, iter) =>
      val dcel = iter.next()
      val r = if(ecells.map(_._2).contains(index)){
        val ecs = ecells.filter(_._2 == index).map{ e =>
          val id = e._1
          val corner = e._3.getEnvelopeInternal
          corner.expandBy(precision)
          (id, envelope2Polygon(corner))
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

    dcelRDD_prime
  }
}
