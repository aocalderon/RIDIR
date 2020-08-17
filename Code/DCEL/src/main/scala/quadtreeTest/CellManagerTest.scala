import scala.collection.JavaConverters._
import com.vividsolutions.jts.geom.{Envelope, Coordinate, LineString}
import com.vividsolutions.jts.geom.GeometryFactory
import com.vividsolutions.jts.io.WKTReader
import org.rogach.scallop._
import scala.io.Source
import java.io.PrintWriter
import edu.ucr.dblab.{StandardQuadTree, QuadRectangle}
import CellManager._

object CellManagerTest {
  def save2(filename: String, content: Seq[String]): Unit = {
    val f = new PrintWriter(filename)
    f.write(content.mkString(""))
    f.close
    println(s"${filename} saved [${content.size} records].")
  }

  def main(args: Array[String]): Unit = {
    val params = new CMTConf(args)
    val geofactory = new GeometryFactory()
    val reader = new WKTReader(geofactory)

    // Reading boundary file...
    val boundaryBuffer = Source.fromFile(params.boundary())
    val boundaryWKT = boundaryBuffer.getLines.next
    boundaryBuffer.close
    val boundary = reader.read(boundaryWKT).getEnvelopeInternal

    // Reading lineages file...
    val lineagesBuffer = Source.fromFile(params.input())
    val lineages = lineagesBuffer.getLines.map{ line =>
        val arr = line.split("\t")
        arr(0)
      }.toList
    lineagesBuffer.close

    // Reading map file...
    val MBuffer = Source.fromFile(params.map())
    val M = MBuffer.getLines.map{ line =>
      val arr = line.split("\t")
      (arr(0).toInt -> arr(1).toInt)
    }.toMap

    val quadtree = Quadtree.create[LineString](boundary, lineages)

    val clean = cleanQuadtree(quadtree, M)

    val ecells = getNextCellWithEdges2(M, clean)


    val cleanWKT = clean.getLeafZones.asScala.map{ leaf =>
      s"${envelope2Polygon(leaf.getEnvelope())}\t${leaf.partitionId}\n"
    }
    save2("/tmp/edgesOutput.wkt", cleanWKT)

    val quadtreeWKT = for{
      q <- quadtree.getLeafZones.asScala.map{ leaf =>
        (leaf.partitionId.toInt, envelope2Polygon(leaf.getEnvelope()), leaf.lineage)
      }.toList
      m <- M.toList if(m._1 == q._1)
        } yield {
      s"${q._2}\t${q._1}\t${q._3}\t${m._2}\n"
    }
    save2("/tmp/edgesInput.wkt", quadtreeWKT)

  }
}

class CMTConf(args: Seq[String]) extends ScallopConf(args) {
  val input: ScallopOption[String] = opt[String] (required = true)
  val boundary: ScallopOption[String] = opt[String] (required = true)
  val map: ScallopOption[String] = opt[String] (required = true)

  verify()
}
