import org.apache.spark.storage.StorageLevel
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.spatialOperator.JoinQuery
import com.vividsolutions.jts.geom.{GeometryFactory}
import com.vividsolutions.jts.io.WKTReader
import org.rogach.scallop._
import org.slf4j.{LoggerFactory, Logger}
import scala.collection.JavaConverters._
import java.io.PrintWriter

object StateSelector{
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val geofactory: GeometryFactory = new GeometryFactory()

  def main(args: Array[String]) = {
    val params     = new StateSelectorConf(args)
    val source     = params.source()
    val target     = params.target()
    val output     = params.output()
    val by         = params.by()
    val search     = params.search().split(",")
    val host       = params.host()
    val port       = params.port()
    val partitions = params.partitions()
    val grid       = params.grid()
    val index      = params.index()
    val cores      = params.cores()
    val executors  = params.executors()
    val debug      = params.debug()
    val local      = params.local()
    var master     = ""
    if(local){
      master = "local[cores]"
    } else {
      master = s"spark://${host}:${port}"
    }
    val gridType = grid match {
      case "QUADTREE"  => GridType.QUADTREE
      case "RTREE"     => GridType.RTREE
      case "EQUALGRID" => GridType.EQUALGRID
      case "KDBTREE"   => GridType.KDBTREE
      case "HILBERT"   => GridType.HILBERT
      case "VORONOI"   => GridType.VORONOI
    }
    val indexType = index match {
      case "QUADTREE"  => IndexType.QUADTREE
      case "RTREE"     => IndexType.RTREE
    }

    // Starting session...
    val totalTime = clocktime
    var timer = clocktime
    val spark = SparkSession.builder().
      config("spark.serializer",classOf[KryoSerializer].getName).
      master(master).appName("Areal").
      config("spark.cores.max", cores * executors).
      config("spark.executor.cores", cores).
      getOrCreate()
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

    // Filtering source...
    val search_position = by match {
      case "state"  => 12
      case "county" => 9
    }

    if(debug){
      val bySearch = sourceRDD.rawSpatialRDD.rdd.map{ s =>
        val arr = s.getUserData.toString().split("\t")
        arr(search_position)
      }.toDF("Search").groupBy($"Search").count().orderBy(desc("count"))
      val nBySearch = bySearch.count()
      log("Counting geoms", timer, nBySearch)
      bySearch.show(30)
    }
    val sample = sourceRDD.rawSpatialRDD.rdd.map{ s =>
      val wkt = s.toText()
      val arr = s.getUserData.toString().split("\t")
      val id = arr(0)
      val tag = arr(search_position)
      val filter = search.contains(tag)
      (id, tag, wkt, filter)
    }.filter(_._4).map(m => (m._1, m._2, m._3)).persist(StorageLevel.MEMORY_ONLY)

    val nSample = sample.count()
    log("Source filtered", timer, nSample)

    // Doing spatial join...
    timer = clocktime
    val considerBoundaryIntersection = true // Only return geometries fully covered by each query window in queryWindowRDD
    val buildOnSpatialPartitionedRDD = true // Set to TRUE only if run join query
    val usingIndex = true

    sourceRDD.setRawSpatialRDD(
      sample.map{ p =>
        val geom = new WKTReader(geofactory).read(p._3)
        geom.setUserData(s"${p._1}\t${p._2}")
        geom
      }
    )
    sourceRDD.analyze()
    sourceRDD.spatialPartitioning(gridType, partitions)
    targetRDD.spatialPartitioning(sourceRDD.getPartitioner)
    sourceRDD.buildIndex(indexType, buildOnSpatialPartitionedRDD)

    val joined = JoinQuery.SpatialJoinQuery(sourceRDD, targetRDD, usingIndex, considerBoundaryIntersection)
    val nJoined = joined.count()
    log("Spatial join done", timer, nJoined)

    // Saving sources...
    timer = clocktime
    val sourceWkts = joined.rdd.map{ pair =>
        val s = pair._1
        val t = pair._2
        val arrSource = s.getUserData.toString().split("\t")
        val arrTarget = t.asScala.head.getUserData.toString().split("\t")
        (arrTarget(1), s"${s.toText()}\t${arrTarget(0)}")
      }
      .toDF("State", "WKT").groupBy($"State")
      .agg(collect_list("WKT").as("WKT"))
    .persist(StorageLevel.MEMORY_ONLY)
    val nSourceWkts = sourceWkts.count()

    for(s <- sourceWkts.collect()){
      val sourceName = s"${output}/${s.getString(0)}_source.wkt"
      val wkt = s.getList(1).asScala.toList.mkString("\n")
      saveWKT(sourceName, wkt)
    }
    log("Sources saved", timer, nSourceWkts)

    // Saving targets...
    timer = clocktime
    val targetWkts = joined.rdd.flatMap(_._2.asScala).map{ t =>
        val arr = t.getUserData.toString().split("\t")
        (arr(1), s"${t.toText()}\t${arr(0)}")
    }.toDF("State", "WKT").distinct().
      groupBy($"State").agg(collect_list("WKT").as("WKT")).
      persist(StorageLevel.MEMORY_ONLY)
    val nTargetWkts = targetWkts.count()

    for(s <- targetWkts.collect()){
      val targetName = s"${output}/${s.getString(0)}_target.wkt"
      val wkt = s.getList(1).asScala.toList.mkString("\n")
      saveWKT(targetName, wkt)
    }
    log("Targets saved", timer, nTargetWkts)

    // Closing session...
    timer = clocktime
    val ttime = "%.2f".format((clocktime - totalTime) / 1000.0)
    logger.info(s"Total execution time: $ttime s")    
    val url = s"http://${host}:4040/api/v1/applications/${appID}/executors"
    val r = requests.get(url)
    if(s"${r.statusCode}" == "200"){
      import scala.util.parsing.json._
      val j = JSON.parseFull(r.text).get.asInstanceOf[List[Map[String, Any]]]
      j.filter(_.get("id").get != "driver").foreach{ m =>
        val tid    = m.get("id").get
        val thost  = m.get("hostPort").get
        val trdds  = "%.0f".format(m.get("rddBlocks").get)
        val ttasks = "%.0f".format(m.get("totalTasks").get)
        val tcores = "%.0f".format(m.get("totalCores").get)
        val ttime  = "%.2fs".format(m.get("totalDuration").get.asInstanceOf[Double] / 1000.0)
        val tinput = "%.2fMB".format(m.get("totalInputBytes").get.asInstanceOf[Double] / (1024.0 * 1024))
        logger.info(s"EXECUTORS;$tcores;$tid;$trdds;$ttasks;$ttime;$tinput;$thost;$appID")
      }
    }

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

  def saveWKT(filename: String, wkt: String): Unit = {
    val pw = new PrintWriter(filename)
    pw.write(wkt)
    pw.close()
  }
}

class StateSelectorConf(args: Seq[String]) extends ScallopConf(args) {
  val source:     ScallopOption[String]  = opt[String]  (required = true)
  val target:     ScallopOption[String]  = opt[String]  (required = true)
  val output:     ScallopOption[String]  = opt[String]  (default = Some("/tmp"))
  val by:         ScallopOption[String]  = opt[String]  (default = Some("state"))
  val search:     ScallopOption[String]  = opt[String]  (default = Some("PA,GA,NY,NC,AZ,WA,CO,WI,LA,IN,AL,TN,OK,SC,CT,MD,NV,IL"))
  val host:       ScallopOption[String]  = opt[String]  (default = Some("169.235.27.138"))
  val port:       ScallopOption[String]  = opt[String]  (default = Some("7077"))
  val cores:      ScallopOption[Int]     = opt[Int]     (default = Some(8))
  val executors:  ScallopOption[Int]     = opt[Int]     (default = Some(3))
  val grid:       ScallopOption[String]  = opt[String]  (default = Some("QUADTREE"))
  val index:      ScallopOption[String]  = opt[String]  (default = Some("QUADTREE"))
  val partitions: ScallopOption[Int]     = opt[Int]     (default = Some(1024))
  val local:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val debug:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))

  verify()
}
