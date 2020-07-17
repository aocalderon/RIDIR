import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.utils.GeoSparkConf
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator
import com.vividsolutions.jts.io.WKTReader
import com.vividsolutions.jts.geom.{Geometry, Polygon, Coordinate, GeometryFactory, PrecisionModel}
import org.locationtech.proj4j.{CRSFactory, CoordinateReferenceSystem, CoordinateTransform, CoordinateTransformFactory, ProjCoordinate}

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

  val partitions = 1

  //val filename = "hdfs:///user/acald013/Datasets/Buildings/sample.tsv"
  val filename = "file:///home/and/Datasets/tmp/sample.tsv"
  val buildingsTXT = spark.read
    .option("header", true).option("delimiter", "\t")
    .csv(filename).rdd

  val csName1 = "EPSG:4326"
  val csName2 = "EPSG:3857"
  val buildingsRaw = buildingsTXT.map{ b =>
    val geofactory = new GeometryFactory()
    val reader = new WKTReader(geofactory)
    val geom = reader.read(b.getString(0))
    val polygon = geom.asInstanceOf[Polygon]

    val ctFactory = new CoordinateTransformFactory()
    val csFactory = new CRSFactory()
    val crs1 = csFactory.createFromName(csName1)
    val crs2 = csFactory.createFromName(csName2)
    val trans = ctFactory.createTransform(crs1, crs2)
    val coords = polygon.getCoordinates.map{ coord =>
      val p1 = new ProjCoordinate(coord.x, coord.y)
      val p2 = new ProjCoordinate()
      trans.transform(p1, p2)
      new Coordinate(p2.x, p2.y)
    }
    val geofactory2 = new GeometryFactory(new PrecisionModel(1000))
    geofactory2.createPolygon(coords)
  }
  val buildingsRDD = new SpatialRDD[Polygon]
  buildingsRDD.setRawSpatialRDD(buildingsRaw)

  if(partitions < 2){
    val f = new java.io.PrintWriter("/tmp/test.wkt")
    val wkt = buildingsRaw.collect().map{ polygon =>
      s"${polygon.toText()}\n"
    }.mkString("")
    f.write(wkt)
    f.close()
  } else {
    buildingsRDD.analyze()
    buildingsRDD.spatialPartitioning(GridType.QUADTREE, partitions)
    
    val output = "hdfs:///user/acald013/buildings"
    buildingsRDD.spatialPartitionedRDD.rdd
      .map(_.toText).toDF("geom").write
      .mode(SaveMode.Overwrite)
      .option("delimiter", "\t")
      .format("csv")
      .save(output)test
   }
  println("Done!")
  spark.close()
}
