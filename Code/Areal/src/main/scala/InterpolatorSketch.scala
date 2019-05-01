import org.apache.spark.storage.StorageLevel
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.rdd.RDD
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.spatialOperator.JoinQuery
import com.vividsolutions.jts.geom.{GeometryFactory, Geometry}
import com.vividsolutions.jts.io.WKTReader
import org.rogach.scallop._
import org.slf4j.{LoggerFactory, Logger}
import scala.collection.JavaConverters._

object InterpolatorSketch{
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val geofactory: GeometryFactory = new GeometryFactory()

  case class Attributes(ID: Int, tarea: Double, population: Int, income: Double, pci: Double)
  val attributeSchema = ScalaReflection.schemaFor[Attributes].dataType.asInstanceOf[StructType]

  var gridType: GridType = GridType.QUADTREE
  var indexType: IndexType = IndexType.QUADTREE
  var partitions: Int = 1024
  var nAreaTable: Long = 0

  def area_table(sourceRDD: SpatialRDD[Geometry], targetRDD: SpatialRDD[Geometry]): RDD[(Int, Int, Double)] = {
    // Doing spatial join...
    var timer = clocktime
    val considerBoundaryIntersection = true // Only return gemeotries fully covered by each query window in queryWindowRDD
    val buildOnSpatialPartitionedRDD = true // Set to TRUE only if run join query
    val usingIndex = true

    sourceRDD.analyze()
    sourceRDD.spatialPartitioning(gridType, partitions)
    targetRDD.spatialPartitioning(sourceRDD.getPartitioner)
    sourceRDD.buildIndex(indexType, buildOnSpatialPartitionedRDD)

    val joined = JoinQuery.SpatialJoinQuery(targetRDD, sourceRDD, usingIndex, considerBoundaryIntersection)
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

  def area_interpolate(spark: SparkSession, sourceRDD: SpatialRDD[Geometry], targetRDD: SpatialRDD[Geometry], extensive_variables: List[String]): Unit = {
    import spark.implicits._

    val areas = area_table(sourceRDD, targetRDD).toDF("SID", "TID", "area")
    areas.show(truncate = false)

    val extensiveAttributes = sourceRDD.rawSpatialRDD.rdd.map{ s =>
      val attr = s.getUserData().toString().split("\t")
      val id = attr(0).toInt
      val tarea = s.getArea()
      val population = attr(1).toInt
      val income = attr(3).toDouble
      (id, tarea, population, income)
    }.toDF("ID", "tarea", "population", "income")

    val table_extensive = areas.join(extensiveAttributes, $"SID" === $"ID")
      .withColumn("tpopulation", $"area" / $"tarea" * $"population")
      .withColumn("tincome", $"area" / $"tarea" * $"income")

    table_extensive.orderBy($"SID").show(truncate = false)

    val target_extensive = table_extensive.select("TID", "tpopulation", "tincome")
      .groupBy($"TID")
      .agg(
        sum($"tpopulation").as("population"),
        sum($"tincome").as("income")
      )

    target_extensive.orderBy($"TID").show(truncate = false)

    val intensiveAttributes = sourceRDD.rawSpatialRDD.rdd.map{ s =>
      val attr = s.getUserData().toString().split("\t")
      val id = attr(0).toInt
      val pci = attr(2).toDouble
      (id, pci)
    }.toDF("IDS", "pci")
    val targetAreas = targetRDD.rawSpatialRDD.rdd.map{ t =>
      val attr = t.getUserData().toString().split("\t")
      val id = attr(0).toInt
      val tarea = t.getArea()
      (id, tarea)
    }.toDF("IDT", "tarea")

    val table_intensive = areas.join(targetAreas, $"TID" === $"IDT", "left_outer")
      .join(intensiveAttributes, $"SID" === $"IDS", "left_outer")
      .withColumn("tpci", $"area" / $"tarea" * $"pci")

    table_intensive.orderBy($"TID").show(truncate = false)

    val target_intensive = table_intensive.select("TID", "tpci")
      .groupBy($"TID")
      .agg(
        sum($"tpci").as("pci")
      )

    target_intensive.orderBy($"TID").show(truncate = false)

  }

  def main(args: Array[String]) = {
    val params     = new IConf(args)
    val source     = params.source()
    val target     = params.target()
    val host       = params.host()
    val port       = params.port()
    val grid       = params.grid()
    val index      = params.index()
    val cores      = params.cores()
    val executors  = params.executors()
    val debug      = params.debug()
    val local      = params.local()
    partitions = params.partitions()
    var master = ""
    if(local){
      master = s"local[$cores]"
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
    timer = clocktime
    val sourceRDD = new SpatialRDD[Geometry]()
    val sourceWKT = spark.read.option("header", "false").option("delimiter", "\t").
      csv(source).rdd.
      map{ s =>
        val geom = new WKTReader(geofactory).read(s.getString(0))
        val id = s.getString(1)
        val population = s.getString(2).toInt
        val pci = s.getString(3).toDouble
        val income = population * pci
        val userData = s"$id\t$population\t$pci\t$income"
        geom.setUserData(userData)
        geom
      }
    sourceRDD.setRawSpatialRDD(sourceWKT)
    val nSourceRDD = sourceRDD.rawSpatialRDD.rdd.count()
    log("Source read", timer, nSourceRDD)

    // Reading target...
    timer = clocktime
    val targetRDD = new SpatialRDD[Geometry]()
    val targetWKT = spark.read.option("header", "false").option("delimiter", "\t").
      csv(target).rdd.
      map{ s =>
        val geom = new WKTReader(geofactory).read(s.getString(0))
        val id = s.getString(1)
        geom.setUserData(id)
        geom
      }
    targetRDD.setRawSpatialRDD(targetWKT)
    val nTargetRDD = targetRDD.rawSpatialRDD.rdd.count()
    log("Target read", timer, nTargetRDD)
    // Calling area_table method...
    val extensive = List("population", "income")
    area_interpolate(spark, sourceRDD, targetRDD, extensive)

    // Reporting results...
    timer = clocktime
    //areal.toDF("SourceID", "TargetID", "Area").show(truncate=false)
    val ttime = "%.2f".format((clocktime - totalTime) / 1000.0)
    logger.info(s"Total execution time: $ttime s")    
    //logger.info(s"AREAL;$cores;$executors;$partitions;$ttime;${areal.count()};$appID")
    if(!local){
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
    }
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

class IConf(args: Seq[String]) extends ScallopConf(args) {
  val source:     ScallopOption[String]  = opt[String]  (required = true)
  val target:     ScallopOption[String]  = opt[String]  (required = true)
  val host:       ScallopOption[String]  = opt[String]  (default = Some("169.235.27.134"))
  val port:       ScallopOption[String]  = opt[String]  (default = Some("7077"))
  val cores:      ScallopOption[Int]     = opt[Int]     (default = Some(4))
  val executors:  ScallopOption[Int]     = opt[Int]     (default = Some(3))
  val grid:       ScallopOption[String]  = opt[String]  (default = Some("QUADTREE"))
  val index:      ScallopOption[String]  = opt[String]  (default = Some("QUADTREE"))
  val partitions: ScallopOption[Int]     = opt[Int]     (default = Some(512))
  val local:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val debug:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))

  verify()
}
