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

    val f = new PrintWriter(params.output())
    val cleanWKT = clean.getLeafZones.asScala.map{ leaf =>
      s"${envelope2Polygon(leaf.getEnvelope())}\t${leaf.partitionId}\n"
    }
    f.write(cleanWKT.mkString(""))
    f.close
    println(s"${params.output()} saved [${cleanWKT.length} records].")

    val i = new PrintWriter("/tmp/edgesInput.wkt")
    val quadtreeWKT = quadtree.getLeafZones.asScala.map{ leaf =>
      s"${envelope2Polygon(leaf.getEnvelope())}\t${leaf.partitionId}\n"
    }
    i.write(quadtreeWKT.mkString(""))
    i.close
    println(s"/tmp/edgesInput.wkt saved [${quadtreeWKT.length} records].")
  }
}

class CMTConf(args: Seq[String]) extends ScallopConf(args) {
  val input: ScallopOption[String] = opt[String] (required = true)
  val boundary: ScallopOption[String] = opt[String] (required = true)
  val map: ScallopOption[String] = opt[String] (required = true)
  val output: ScallopOption[String] = opt[String](default = Some("/tmp/edgesOutput.wkt"))

  verify()
}
