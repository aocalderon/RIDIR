import org.datasyslab.geospark.spatialPartitioning.quadtree.{StandardQuadTree, QuadRectangle}
import org.apache.spark.rdd.RDD
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.geom.{Coordinate, Point, LineString}
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

object CellManager{
  private val model: PrecisionModel = new PrecisionModel(1000)
  private val geofactory: GeometryFactory = new GeometryFactory(model);
  private val precision: Double = 1 / model.getScale

  def getCellsAtCorner(quadtree: StandardQuadTree[LineString], c: QuadRectangle): (List[QuadRectangle], Point) = {
    val region = c.lineage.takeRight(1).toInt
    val corner = region match {
      case 0 => geofactory.createPoint(new Coordinate(c.getEnvelope.getMaxX, c.getEnvelope.getMinY))
      case 1 => geofactory.createPoint(new Coordinate(c.getEnvelope.getMinX, c.getEnvelope.getMinY))
      case 2 => geofactory.createPoint(new Coordinate(c.getEnvelope.getMaxX, c.getEnvelope.getMaxY))
      case 3 => geofactory.createPoint(new Coordinate(c.getEnvelope.getMinX, c.getEnvelope.getMaxY))
    }
    val envelope = corner.getEnvelopeInternal
    envelope.expandBy(precision)
    val cells = quadtree.findZones(new QuadRectangle(envelope)).asScala
      .filterNot(_.partitionId == c.partitionId).toList
      .sortBy(_.lineage.size)
    (cells, corner)
  }

  def getNextCellWithEdges(MA: Map[Int, Int], quadtree: StandardQuadTree[LineString]): List[(Int, Int, Point)] = {
    val cells = quadtree.getLeafZones.asScala.map(cell => (cell.partitionId.toInt -> cell)).toMap
    var result = new ListBuffer[(Int, Int, Point)]()
    MA.filter{ case(index, size) => size == 0 }.map{ case(index, size) =>
      var cellList = new ListBuffer[QuadRectangle]()
      var nextCellWithEdges = -1
      var referenceCorner = geofactory.createPoint(new Coordinate(0,0))
      cellList += cells(index)
      var done = false
      while(!done){
        val c = cellList.last
        val (ncells, corner) = getCellsAtCorner(quadtree, c)
        for(cell <- ncells){
          val nedges = MA(cell.partitionId)
          if(nedges > 0){
            nextCellWithEdges = cell.partitionId
            referenceCorner = corner
            done = true
          } else {
            cellList += cell
          }
        }
      }
      for(cell <- cellList){
        val r = (cell.partitionId.toInt, nextCellWithEdges, referenceCorner)
        result += r
      }
    }
    result.toList
  }

  def updateCellsWithoutId(dcelRDD: RDD[LDCEL], quadtree: StandardQuadTree[LineString]): RDD[LDCEL] = {
    val M = dcelRDD.mapPartitionsWithIndex{ case(index, iter) =>
      val dcel = iter.next()
      val r = (index, dcel.half_edges.filter(_.id.substring(0,1) != "F").size)
      Iterator(r)
    }.collect().toMap

    val ecells = getNextCellWithEdges(M, quadtree)
    val fcells = dcelRDD.mapPartitionsWithIndex{ case(index, iter) =>
      val dcel = iter.next()
      val r = if(ecells.map(_._2).contains(index)){
        val ecs = ecells.filter(_._2 == index).map{ e =>
          val id = e._1
          val corner = e._3.buffer(precision)
          (id, corner)
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
    }.collect()
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
    dcelRDD_prime
  }
}
