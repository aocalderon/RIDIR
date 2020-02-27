import org.datasyslab.geospark.spatialPartitioning.quadtree.{StandardQuadTree, QuadRectangle}
import org.apache.spark.rdd.RDD
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.geom.{Envelope, Coordinate, Point, LineString}
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._
import DCELMerger.timer
import DCELBuilder.envelope2Polygon

case class Cell(id: Int, lineage: String, envelope: Envelope)

object CellManager{
  private val model: PrecisionModel = new PrecisionModel(100)
  private val geofactory: GeometryFactory = new GeometryFactory(model);
  private val precision: Double = 1 / model.getScale

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

  def getNextCellWithEdges(M: Map[Int, Int], quadtree: StandardQuadTree[LineString], grids: Map[Int, QuadRectangle]): List[(Int, Int, Point)] = {
    val cells = grids.map(grid => grid._1 -> Cell(grid._2.partitionId.toInt, grid._2.lineage, grid._2.getEnvelope)).toMap
    var result = new ListBuffer[(Int, Int, Point)]()
    var R = new ListBuffer[Int]()

    val Z = M.filter{ case(index, size) => size == 0 }.map(_._1).toVector.sorted
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
          println(s"Current cell: ${c.id} [${c.lineage}]")
          val (ncells, corner) = getCellsAtCorner(quadtree, c)

          val ncells_prime = ncells.par.map(c => (c, M(c.id))).filter(_._2 != 0)
          val hasEdges = if(ncells_prime.size == 0) None else Some(ncells_prime.head._1)
          done = hasEdges match {
            case Some(cell) => {
              println(s"Found a cell at ${cell.id} and ${corner}")
              nextCellWithEdges = cell.id
              referenceCorner = corner
              true
            }
            case None => false
          }

          if(!done){
            cellList ++= ncells
            println(s"cellList: ${cellList.mkString(" ")}")
          }

          /*
           for(cell <- ncells){
           val nedges = M(cell.id)
           println(s"Evaluating $cell: $nedges")
           if(nedges > 0){
           nextCellWithEdges = cell.id
           referenceCorner = corner
           println(s"Done! $nextCellWithEdges")
           done = true
           } else {
           cellList += cell
           }
           }
           */
          if(!done && cellList.takeRight(4).map(_.lineage.size).distinct.size == 1){
            println(s"4 empty cells detected around cell ${c.id}:")
            val quads = cellList.takeRight(4).sortBy(_.lineage)
            quads.map(c => s"${c.id}: [${c.lineage}]").foreach{println}
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

            println("New cell list:")
            cellList.foreach{println}
          }
        }
        for(cell <- cellList){
          R += cell.id
          val r = (cell.id, nextCellWithEdges, referenceCorner)
          result += r

          //println(s"Z ==> ${Z.mkString(" ")} ")
          //println(s"R ==> ${R.mkString(" ")} ")
        }
        R = R.sorted
      }
    }
    result.toList
  }

  def updateCellsWithoutId(dcelRDD: RDD[LDCEL], quadtree: StandardQuadTree[LineString], grids: Map[Int, QuadRectangle]): RDD[LDCEL] = {
    val M = dcelRDD.mapPartitionsWithIndex{ case(index, iter) =>
      val dcel = iter.next()
      val r = (index, dcel.half_edges.filter(_.id.substring(0,1) != "F").size)
      Iterator(r)
    }.collect().toMap
    println("Getting M...")

    val ecells = getNextCellWithEdges(M, quadtree, grids)
    println("getNextCell...")

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
          fcell <- fcs
        } yield {
          println(s"To eval: $ecell <-> $fcell")
        }

        for{
          ecell <- ecs
          fcell <- fcs if fcell._2.intersects(ecell._2)
        } yield {
          println(s"Eval: ${ecell._1} <-> ${fcell._1}")
          (ecell._1, fcell._1)
        }
      } else {
        Vector.empty[(Int, String)]
      }
      r.toIterator
    }.collect()
    println("fcells...")

    val dcelRDD_prime = dcelRDD.mapPartitionsWithIndex{ case(index, iter) =>
      val dcel = iter.next()
      if(fcells.map(_._1).contains(index)){
        val cell = fcells.filter(_._1 == index)
        val id = cell.head._2
        val hedges = dcel.half_edges.map{ h =>
          h.id = id
          h
        }
        val face = Face(id, index)
        face.id = id
        face.outerComponent = hedges.head
        val faces = Vector(face)
        val d = LDCEL(index, dcel.vertices, hedges, faces)
        Iterator(d)
      } else {
        Iterator(dcel)
      }
    }
    println("dcel_prime...")

    dcelRDD_prime
  }
}
