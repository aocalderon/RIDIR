import org.apache.spark.sql.SparkSession
import org.locationtech.jts.index.quadtree._
import scala.collection.JavaConverters._

object StarkTester {
  def main(args: Array[String]): Unit = {
    val input = "/home/acald013/Datasets/polygons.txt"
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val tree = new Quadtree()
    spark.close()
  }
}
