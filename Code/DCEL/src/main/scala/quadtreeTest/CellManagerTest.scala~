import scala.collection.JavaConverters._
import com.vividsolutions.jts.geom.{Envelope, Coordinate, LineString}
import com.vividsolutions.jts.geom.GeometryFactory
import com.vividsolutions.jts.io.WKTReader
import scala.io.Source
import edu.ucr.dblab.{StandardQuadTree, QuadRectangle}
import CellManager._

object CellManagerTest {
  case class Cell(lineage: String, count: Int)

  def main(args: Array[String]): Unit = {
    val path = "/home/acald013/RIDIR/Code/DCEL/src/test/resources"
    val geofactory = new GeometryFactory()
    val reader = new WKTReader(geofactory)
    val boundaryBuffer = Source.fromFile(s"${path}/envelope.tsv")
    val boundaryWKT = boundaryBuffer.getLines.next
    boundaryBuffer.close

    val boundary = reader.read(boundaryWKT)

    val cellsBuffer = Source.fromFile(s"${path}/quadtree.tsv")
    val cells = cellsBuffer.getLines.map{ line =>
        val arr = line.split("\t")
        Cell(arr(0), arr(1).toInt)
      }.toList
    cellsBuffer.close

    val lineages = cells.map(_.lineage)
    lineages.take(5).foreach{println}
    val quadtree = Quadtree.create[LineString](boundary.getEnvelopeInternal, lineages)

    val MBuffer = Source.fromFile(s"${path}/MB.tsv")
    val M = MBuffer.getLines.map{ line =>
      val arr = line.split("\t")
      (arr(0).toInt -> arr(1).toInt)
    }.toMap

    val grids = quadtree.getLeafZones.asScala.map{ leaf =>
      leaf.partitionId.toInt -> leaf
    }.toMap
    getNextCellWithEdges2(M: Map[Int, Int], quadtree: StandardQuadTree[LineString], grids: Map[Int, QuadRectangle])

  }
}
