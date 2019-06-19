import org.slf4j.{LoggerFactory, Logger}
import org.rogach.scallop._
import org.dyn4j.geometry.decompose._
import org.dyn4j.geometry.Vector2
import scala.collection.JavaConverters._

object Tester{
  private val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]) = {
    val params = new TesterConf(args)
    val debug = params.debug()

    var timer = clocktime
    logger.info(s"Hello world...")
    val p1 = new Vector2(0.0, 0.0)
    val p2 = new Vector2(1.0, 0.0)
    val p3 = new Vector2(1.0, 1.0)
    val p4 = new Vector2(0.0, 1.0)
    val p5 = new Vector2(-0.5, 0.5)
    val points = List(p1,p2,p3,p4,p5)
    val s = new SweepLine()
    val dcel = s.triangulate(points: _*)
    dcel.asScala.toList.map{ c =>
      s"Convex: ${c.toString()}"
    }.foreach(println)
    
  }

  def clocktime = System.currentTimeMillis()

  def log(msg: String, timer: Long, n: Long = 0): Unit = {
    logger.info("%-50s|%6.2f|%6d".format(msg,(clocktime - timer)/1000.0,n))
  }
}

class TesterConf(args: Seq[String]) extends ScallopConf(args) {
  val debug:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))

  verify()
}
