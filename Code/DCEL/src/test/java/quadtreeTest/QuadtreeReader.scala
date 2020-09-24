import scala.collection.JavaConverters._
import com.vividsolutions.jts.geom.{Envelope, Coordinate}
import com.vividsolutions.jts.geom.GeometryFactory
import com.vividsolutions.jts.io.WKTReader
import scala.io.Source
import edu.ucr.dblab.{StandardQuadTree, QuadRectangle}

object QuadtreeReader {
  case class Cell(lineage: String, count: Int)

  def main(args: Array[String]): Unit = {
    val geofactory = new GeometryFactory()
    val reader = new WKTReader(geofactory)
    val boundaryBuffer = Source.fromFile("/tmp/envelope.tsv")
    val boundaryWKT = boundaryBuffer.getLines.next
    boundaryBuffer.close

    println(boundaryWKT)
    val boundary = reader.read(boundaryWKT)

    val cells = Source.fromFile("/tmp/quadtree.tsv")
      .getLines.map{ line =>
        val arr = line.split("\t")
        Cell(arr(0), arr(1).toInt)
      }.toList

    val lineages = cells.map(_.lineage)
    lineages.take(5).foreach{println}
    val quadtree = Quadtree.create(boundary.getEnvelopeInternal, lineages)

    /////////////////////////////////////////////////////////////////////
    val grids2 = quadtree.getLeafZones.asScala
    val wkt2 = grids2.map{ g =>
      val x1 = g.getEnvelope.getMinX()
      val x2 = g.getEnvelope.getMaxX()
      val y1 = g.getEnvelope.getMinY()
      val y2 = g.getEnvelope.getMaxY()
      val coords = Array(
        new Coordinate(x1,y1),
        new Coordinate(x2,y1),
        new Coordinate(x2,y2),
        new Coordinate(x1,y2),
        new Coordinate(x1,y1)
      )
      val polygon = geofactory.createPolygon(coords)
      polygon.setUserData(s"${g.partitionId}\t${g.lineage}")

      s"${polygon.toText()}\t${polygon.getUserData.toString()}\n"
    }
    val f2 = new java.io.PrintWriter("/tmp/edgesEnvelopes2.wkt")
    f2.write(wkt2.mkString(""))
    f2.close()
    /////////////////////////////////////////////////////////////////////

  }
}
