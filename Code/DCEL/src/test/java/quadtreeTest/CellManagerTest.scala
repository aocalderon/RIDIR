import scala.collection.immutable.HashMap
import scala.collection.JavaConverters._
import com.vividsolutions.jts.geom.{Envelope, Coordinate, LineString}
import com.vividsolutions.jts.geom.GeometryFactory
import com.vividsolutions.jts.io.WKTReader
import org.rogach.scallop._
import scala.io.Source
import java.io.PrintWriter
import edu.ucr.dblab.{StandardQuadTree, QuadRectangle}
import DCELMerger.envelope2polygon
import CellManager2._

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

    // Reading quadtree file...
    val lineagesBuffer = Source.fromFile(params.quadtree())
    val lineages = lineagesBuffer.getLines.map{ line =>
        val arr = line.split("\t")
        arr(0)
      }.toList
    lineagesBuffer.close

    val quadtree = Quadtree.create[LineString](boundary, lineages)

    // Reading empty cells file...
    val EBuffer = Source.fromFile(params.empties())
    val E = EBuffer.getLines.map{ line =>
      val arr = line.split("\t")
      arr(0)
    }.toSet

    // Getting the empty and non-empty cell sets...
    val X = quadtree.getLeafZones.asScala.map{ leaf =>
      val cell = Cell(leaf.partitionId.toInt, leaf.lineage, leaf.getEnvelope)
      cell.lineage -> cell
    }.toList.partition{ cell => E.contains(cell._1)}

    val empties = HashMap(X._1:_*)
    val nonempties = HashMap(X._2:_*)

    // Cleaning the quadtree...
    val (quadtree_prime, empties_prime) = cleanQuadtreeTester(quadtree, empties, nonempties)

    // Printing data...
    val empties_primeTSV = empties_prime.values.map(e => e.lineage + "\n").toList
    save2("/tmp/empties_prime.tsv", empties_primeTSV)

    val quadtree_primeWKT = quadtree_prime.getLeafZones.asScala.map{ leaf =>
      Cell(leaf.partitionId.toInt, leaf.lineage, leaf.getEnvelope)
    }.map{ cell =>
      val E = empties_prime.values.map(_.lineage).toSet
      val isEmpty = E.contains(cell.lineage)
      s"${envelope2polygon(cell.boundary)}\t${cell.id}\t${cell.lineage}\t${isEmpty}\n"
    }    
    save2("/tmp/edgesOutput.wkt", quadtree_primeWKT)

    val quadtreeWKT = quadtree.getLeafZones.asScala.map{ leaf =>
      Cell(leaf.partitionId.toInt, leaf.lineage, leaf.getEnvelope)
    }.map{ cell =>
      val isEmpty = E.contains(cell.lineage)
      s"${envelope2polygon(cell.boundary)}\t${cell.id}\t${cell.lineage}\t${isEmpty}\n"
    }    
    save2("/tmp/edgesInput.wkt", quadtreeWKT)

    val closestCells = getClosestCell(quadtree_prime, empties_prime, debug = true)

    closestCells.foreach{println}

  }
}

class CMTConf(args: Seq[String]) extends ScallopConf(args) {
  val quadtree: ScallopOption[String] = opt[String] (required = true)
  val boundary: ScallopOption[String] = opt[String] (required = true)
  val empties:  ScallopOption[String] = opt[String] (required = true)

  verify()
}
