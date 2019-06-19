import org.slf4j.{LoggerFactory, Logger}
import org.rogach.scallop._
import java.io._

object Tester{
  private val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]) = {
    val params = new TesterConf(args)
    val debug = params.debug()

    var timer = clocktime
    logger.info(s"Hello world...")
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
