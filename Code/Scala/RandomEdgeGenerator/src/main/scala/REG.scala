import org.locationtech.jts.geom.{Coordinate, LineString}
import org.locationtech.jts.geom.{GeometryFactory, PrecisionModel}
import org.rogach.scallop.ScallopConf
import org.slf4j.{Logger, LoggerFactory}
import scala.util.Random
import edu.ucr.dblab.Utils.save

object REG {
  implicit val logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]): Unit = {
    val geofactory = new GeometryFactory(new PrecisionModel(1000))
    val params = new REGConf(args)
    val n = params.n()
    val corners = params.boundary().split(" ").map(_.toDouble)
    val minX = corners(0)
    val minY = corners(1)
    val maxX = corners(2)
    val maxY = corners(3)
    val width  = maxX - minX
    val height = maxY - minY
    /*
    val p1 = new Coordinate(minX, minY)
    val p2 = new Coordinate(maxX, minY)
    val p3 = new Coordinate(maxX, maxY)
    val p4 = new Coordinate(minX, maxY)
    val boundary = geofactory.createPolygon(Array(p1,p2,p3,p4,p1))
     */
    val random = if(params.seed() != 0){
      new Random()
    } else {
      new Random(params.seed())
    }

    val lines = (0 until n).toList.par.map{ i =>
      val x1 = minX + width  * random.nextDouble()
      val y1 = minY + height * random.nextDouble()
      val x2 = minX + width  * random.nextDouble()
      val y2 = minY + height * random.nextDouble()
      val p1 = new Coordinate(x1, y1)
      val p2 = new Coordinate(x2, y2)

      val line = geofactory.createLineString(Array(p1,p2))
      line.setUserData(i)

      line
    }

    save{"/tmp/edgesEdges.wkt"}{
      lines.map{ line =>
        s"${line.toText()}\t${line.getUserData.toString()}\n"
      }.seq
    }
  }
}

class REGConf(args: Seq[String]) extends ScallopConf(args) {
  val boundary = opt[String](default = Some("0 0 1000 1000"))
  val n = opt[Int](default = Some(100))
  val seed = opt[Long](default = Some(0))

  verify()
}
