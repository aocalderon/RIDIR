import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql._
import org.slf4j.{LoggerFactory, Logger}
import org.rogach.scallop._
import scala.collection.JavaConverters._
import java.io._

object TestValidator{
  private val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]) = {
    val params     = new TestValidatorConf(args)
    val input      = params.input()
    val state      = params.state()
    val host       = params.host()
    val port       = params.port()
    val partitions = params.partitions()
    val cores      = params.cores()
    val executors  = params.executors()

    // Starting session...
    val spark = SparkSession.builder().
      master("local[*]").appName("TestValidator").
      getOrCreate()
    import spark.implicits._
    val appID = spark.sparkContext.applicationId
    logger.info(s"Session $appID started")    

    // Reading data...
    val geopandas = spark.read.option("header", "false").option("delimiter", "\t").csv(s"${input}/${state}_geopandas_test.tsv").distinct()
    geopandas.count()
    val geospark = spark.read.option("header", "false").option("delimiter", "\t").csv(s"${input}/${state}_geospark_test.tsv").distinct()
    geospark.count()
    val p = geopandas.toDF("s1", "t1", "area1").orderBy("s1", "t1", "area1")
    p.show()
    val s = geopandas.toDF("s2", "t2", "area2").orderBy("s2", "t2","area2")
    s.show()
    logger.info("Files read")

    val a = p.select("area1").map(_.getString(0).toDouble).rdd.zip(s.select("area2").map(_.getString(0).toDouble).rdd)

    import org.apache.spark.mllib.evaluation.RegressionMetrics
    val reg = new RegressionMetrics(a)
    val r2 = reg.r2
    val mar = reg.meanAbsoluteError
    val mse = reg.meanSquaredError
    val rmse = reg.rootMeanSquaredError
    logger.info(s"R2: $r2\tMAR: $mar\tMSE: $mse\tRMSE: $rmse")

    spark.close()
    logger.info(s"Session $appID closed")
  }
}

class TestValidatorConf(args: Seq[String]) extends ScallopConf(args) {
  val input:      ScallopOption[String]  = opt[String]  (default = Some("/home/acald013/Datasets/ArealTests"))
  val state:      ScallopOption[String]  = opt[String]  (default = Some("MD"))
  val host:       ScallopOption[String]  = opt[String]  (default = Some("169.235.27.138"))
  val port:       ScallopOption[String]  = opt[String]  (default = Some("7077"))
  val cores:      ScallopOption[Int]     = opt[Int]     (default = Some(8))
  val executors:  ScallopOption[Int]     = opt[Int]     (default = Some(3))
  val partitions: ScallopOption[Int]     = opt[Int]     (default = Some(1024))
  val local:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))

  verify()
}
