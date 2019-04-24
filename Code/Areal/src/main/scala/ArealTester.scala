import org.apache.spark.storage.StorageLevel
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.spatialOperator.JoinQuery
import com.vividsolutions.jts.geom.Geometry
import org.rogach.scallop._
import org.slf4j.{LoggerFactory, Logger}
import scala.collection.JavaConverters._

object ArealTester{
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  var gridType: GridType = GridType.QUADTREE
  var indexType: IndexType = IndexType.QUADTREE
  var partitions: Int = 1024
  var nAreaTable: Long = 0

  def area_table(sourceRDD: SpatialRDD[Geometry], targetRDD: SpatialRDD[Geometry], sample: Double = 1.0): RDD[(Int, Int, Double)] = {
    // Doing spatial join...
    var timer = clocktime
    val considerBoundaryIntersection = true // Only return gemeotries fully covered by each query window in queryWindowRDD
    val buildOnSpatialPartitionedRDD = true // Set to TRUE only if run join query
    val usingIndex = true

    sourceRDD.setRawSpatialRDD(sourceRDD.rawSpatialRDD.rdd.sample(false, sample, 42))
    targetRDD.setRawSpatialRDD(targetRDD.rawSpatialRDD.rdd.sample(false, sample, 42))

    sourceRDD.analyze()
    sourceRDD.spatialPartitioning(gridType, partitions)
    targetRDD.spatialPartitioning(sourceRDD.getPartitioner)
    sourceRDD.buildIndex(indexType, buildOnSpatialPartitionedRDD)

    val joined = JoinQuery.SpatialJoinQuery(sourceRDD, targetRDD, usingIndex, considerBoundaryIntersection)
    val nJoined = joined.count()
    log("Spatial join done", timer, nJoined)

    // Flattening join results...
    val flattened = joined.rdd.flatMap{ pair =>
      val a = pair._1
      pair._2.asScala.map(b => (a, b))
    }
    val nFlattened = flattened.count()
    log("Join results flattened", timer, nFlattened)

    // Computing intersection area...
    val areal = flattened.map{ pair =>
      val source_id  = pair._1.getUserData.toString().split("\t")(0).toInt
      val target_id  = pair._2.getUserData.toString().split("\t")(0).toInt
      val area = pair._1.intersection(pair._2).getArea
      (source_id, target_id, area)
    }
    nAreaTable = areal.count()
    log("Area intersection computed", timer, nAreaTable)

    areal
  }

  def main(args: Array[String]) = {
    val params     = new ArealTesterConf(args)
    val source     = params.source()
    val target     = params.target()
    val sample     = params.sample()
    val host       = params.host()
    val port       = params.port()
    val grid       = params.grid()
    val index      = params.index()
    val cores      = params.cores()
    val executors  = params.executors()
    val debug      = params.debug()
    val local      = params.local()
    partitions = params.partitions()
    var master     = ""
    if(local){
      master = "local[cores]"
    } else {
      master = s"spark://${host}:${port}"
    }
    gridType = grid match {
      case "QUADTREE"  => GridType.QUADTREE
      case "RTREE"     => GridType.RTREE
      case "EQUALGRID" => GridType.EQUALGRID
      case "KDBTREE"   => GridType.KDBTREE
      case "HILBERT"   => GridType.HILBERT
      case "VORONOI"   => GridType.VORONOI
    }
    indexType = index match {
      case "QUADTREE"  => IndexType.QUADTREE
      case "RTREE"     => IndexType.RTREE
    }

    // Starting session...
    val totalTime = clocktime
    var timer = clocktime
    val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .master(master).appName("Areal")
      .config("spark.cores.max", cores * executors)
      .config("spark.executor.cores", cores)
      .getOrCreate()
    import spark.implicits._
    val appID = spark.sparkContext.applicationId
    logger.info(s"Session $appID started [${(clocktime - timer) / 1000.0}]")    

    // Reading source...
    val sourceRDD = ShapefileReader.readToGeometryRDD(spark.sparkContext, source)
    val sourceIDs = sourceRDD.rawSpatialRDD.rdd.zipWithUniqueId.map{ s =>
      val id = s._2
      val geom = s._1
      geom.setUserData(s"${id}\t${geom.getUserData.toString()}")
      geom
    }.persist(StorageLevel.MEMORY_ONLY)
    sourceRDD.setRawSpatialRDD(sourceIDs)
    val nSourceRDD = sourceRDD.rawSpatialRDD.rdd.count()
    log("Source read", timer, nSourceRDD)

    // Reading target...
    timer = clocktime
    val targetRDD = ShapefileReader.readToGeometryRDD(spark.sparkContext, target)
    val targetIDs = targetRDD.rawSpatialRDD.rdd.zipWithUniqueId.map{ t =>
      val id = t._2
      val geom = t._1
      geom.setUserData(s"${id}\t${geom.getUserData.toString()}")
      geom
    }.persist(StorageLevel.MEMORY_ONLY)
    targetRDD.setRawSpatialRDD(targetIDs)
    val nTargetRDD = targetRDD.rawSpatialRDD.rdd.count()
    log("Target read", timer, nTargetRDD)

    // Calling area_table method...
    val areal = area_table(sourceRDD, targetRDD, sample)

    // Reporting results...
    timer = clocktime
    areal.toDF("SourceID", "TargetID", "Area").show(truncate=false)
    val ttime = "%.2f".format((clocktime - totalTime) / 1000.0)
    logger.info(s"Total execution time: $ttime s")    
    logger.info(s"AREAL|$cores|$executors|$partitions|$ttime|${areal.count()}|$sample|$appID")

    // Closing session...
    spark.close()
    logger.info(s"Session $appID closed [${(clocktime - timer) / 1000.0}]")    
  }

  def clocktime = System.currentTimeMillis()

  def log(msg: String, timer: Long, n: Long = -1): Unit = {
    if(n == -1)
      logger.info("%-50s|%6.2f".format(msg, (clocktime - timer)/1000.0))
    else
      logger.info("%-50s|%6.2f|%6d".format(msg, (clocktime - timer)/1000.0, n))
  }
}

class ArealTesterConf(args: Seq[String]) extends ScallopConf(args) {
  val source:     ScallopOption[String]  = opt[String]  (required = true)
  val target:     ScallopOption[String]  = opt[String]  (required = true)
  val host:       ScallopOption[String]  = opt[String]  (default = Some("169.235.27.134"))
  val port:       ScallopOption[String]  = opt[String]  (default = Some("7077"))
  val sample:     ScallopOption[Double]  = opt[Double]  (default = Some(0.1))
  val cores:      ScallopOption[Int]     = opt[Int]     (default = Some(4))
  val executors:  ScallopOption[Int]     = opt[Int]     (default = Some(3))
  val grid:       ScallopOption[String]  = opt[String]  (default = Some("QUADTREE"))
  val index:      ScallopOption[String]  = opt[String]  (default = Some("QUADTREE"))
  val partitions: ScallopOption[Int]     = opt[Int]     (default = Some(512))
  val local:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val debug:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))

  verify()
}
