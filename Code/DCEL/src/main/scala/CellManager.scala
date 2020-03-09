import org.datasyslab.geospark.spatialPartitioning.quadtree.{StandardQuadTree, QuadRectangle}
import org.apache.spark.rdd.RDD
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.geom.{Envelope, Coordinate, Point, LineString}
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._
import DCELMerger.{timer, geofactory, precision}
import DCELBuilder.envelope2Polygon

case class Cell(id: Int, lineage: String, envelope: Envelope)

object CellManager{

  def getEmptyCells(dcelRDD: RDD[LDCEL], grids: Map[Int, QuadRectangle]): Map[Int, Int] = {
    dcelRDD.mapPartitionsWithIndex{ case(index, iter) =>
      val dcel = iter.next()
      val faces = dcel.faces.filter(_.id.substring(0, 1) == "F")
      val m = if(faces.size == 0){
        println(s"Cell: $index 1 Faces: ${faces.map(_.id).mkString(" ")}")
        (index, 1)
      } else {
        val cellFace = envelope2Polygon(grids(index).getEnvelope)
        val equals = faces.filter { face =>
          val compare = face.toPolygon().equals(cellFace)
          println(s"Cell: $index Compare: $compare \n${face.toPolygon.toText()}\n${cellFace.toText()}")
          face.toPolygon().equals(cellFace)
        }
        if(equals.isEmpty){
          println(s"Cell: $index 1 Faces: ${faces.map(_.id).mkString(" ")}")
          (index, 1)
        } else {
          println(s"Cell: $index 0 Faces: ${faces.map(_.id).mkString(" ")}")
          (index, 0)
        }
      }
      Iterator(m)
    }.collect().toMap
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

  def getNextCellWithEdges(M: Map[Int, Int], quadtree: StandardQuadTree[LineString], grids: Map[Int, QuadRectangle]): List[(Int, Int, Point)] = {
    val cells = grids.map(grid => grid._1 -> Cell(grid._2.partitionId.toInt, grid._2.lineage, grid._2.getEnvelope)).toMap
    var result = new ListBuffer[(Int, Int, Point)]()
    var R = new ListBuffer[Int]()

    val Z = M.filter{ case(index, size) => size == 0 }.map(_._1).toVector.sorted

    println("Printing Z...")
    Z.foreach(println)

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
          println(s"$c => IDs: ${ncells.map(_.id).mkString(" ")} Corner: ${corner.toText()}")

          val ncells_prime = ncells.par.map(c => (c, M(c.id))).filter(_._2 != 0)

          //
          ncells_prime.foreach(println)

          val hasEdges = if(ncells_prime.size == 0) None else Some(ncells_prime.head._1)

          //
          println(hasEdges)

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

  def updateCellsWithoutId(dcelRDD: RDD[LDCEL], quadtree: StandardQuadTree[LineString], grids: Map[Int, QuadRectangle]): RDD[LDCEL] = {
    /*
    val M = dcelRDD.mapPartitionsWithIndex{ case(index, iter) =>
      val dcel = iter.next()
      val r = (index, dcel.half_edges.filter(_.id.substring(0,1) != "F").size)
      Iterator(r)
     }.collect().toMap*/

    val M = getEmptyCells(dcelRDD: RDD[LDCEL], grids: Map[Int, QuadRectangle])

    val ecells = getNextCellWithEdges(M, quadtree, grids)

    //
    ecells.foreach { println }

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
          fcell <- fcs if fcell._2.intersects(ecell._2)
        } yield {
          (ecell._1, fcell._1)
        }
      } else {
        Vector.empty[(Int, String)]
      }
      r.toIterator
    }.collect().filter(_._2.substring(0, 1) != "F")

    //
    fcells.foreach { println }

    val dcelRDD_prime = dcelRDD.mapPartitionsWithIndex{ case(index, iter) =>
      val dcel = iter.next()
      if(fcells.map(_._1).contains(index)){
        val cell = fcells.filter(_._1 == index)
        val id = cell.head._2
        val hedges = dcel.half_edges.filter(_.id.substring(0,1) == "F").map{ h =>
          h.id = id
          h
        }
        //val hedgesA = dcel.half_edges.filter(_.id.substring(0,1) != "F")
        //val hedges = hedgesA ++ hedgesF

        dcel.faces.filter(_.id.substring(0,1) == "F").map{ f =>
          f.id = id
          f.outerComponent = hedges.head
          f
        }
        //val facesA = dcel.faces.filter(_.id.substring(0,1) != "F")
        //val faces = facesA ++ facesF
        val d = LDCEL(index, dcel.vertices, dcel.half_edges, dcel.faces, dcel.index)

        //
        dcel.faces.foreach{println}
        println(d)

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
