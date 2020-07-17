import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.utils.GeoSparkConf
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator
import com.vividsolutions.jts.io.WKTReader
import com.vividsolutions.jts.geom.{Geometry, GeometryFactory, PrecisionModel}
import org.opengis.referencing.crs.CoordinateReferenceSystem
import org.opengis.referencing.operation.MathTransform
import org.geotools.referencing.CRS
import org.geotools.geometry.jts.JTS

object Translate extends App{
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val spark = SparkSession.builder()
    .config("spark.serializer",classOf[KryoSerializer].getName)
    .config("spark.kryo.registrator", classOf[GeoSparkVizKryoRegistrator].getName)
    .appName("GeoTemplate")
    .getOrCreate()

  GeoSparkSQLRegistrator.registerAll(spark)
  import spark.implicits._

  val partitions = 1024

  val filename = "hdfs:///user/acald013/Datasets/Buildings/sample.tsv"
  val buildingsTXT = spark.read
    .option("header", true).option("delimiter", "\t")
    .csv(filename).rdd

  val buildingsRaw = buildingsTXT.map{ b =>
    val sourceCRS = CRS.decode("EPSG:4326")
    val targetCRS = CRS.decode("EPSG:6414")
    val lenient = true // allow for some error due to different datums
    val transform = CRS.findMathTransform(sourceCRS, targetCRS, lenient)
    val geofactory = new GeometryFactory()
    val reader = new WKTReader(geofactory)
    val geom = reader.read(b.getString(0))
    JTS.transform(geom, transform)
  }
  val buildingsRDD = new SpatialRDD[Geometry]
  buildingsRDD.setRawSpatialRDD(buildingsRaw)

  buildingsRDD.analyze()
  buildingsRDD.spatialPartitioning(GridType.QUADTREE, partitions)

  val output = "hdfs:///user/acald013/buildings"
  buildingsRDD.spatialPartitionedRDD.rdd
    .map(_.toText).toDF("geom").write
    .mode(SaveMode.Overwrite)
    .option("delimiter", "\t")
    .format("csv")
    .save(output)
  println("Done!")
  spark.close()
}
