import org.slf4j.{LoggerFactory, Logger}
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
import org.rogach.scallop._

object Translate {
  def main(args: Array[String]): Unit = {
    implicit val logger: Logger = LoggerFactory.getLogger("myLogger")
    logger.info("Starting session...")
    val params = new TranslateConf(args)
    val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkVizKryoRegistrator].getName)
      .appName("GeoTemplate")
      .getOrCreate()

    GeoSparkSQLRegistrator.registerAll(spark)
    import spark.implicits._
    logger.info("Starting session... Done!")

    val input = params.input()
    logger.info(s"Reading ${input}...")
    val partitions = params.partitions()
    val geometriesTXT = spark.read
      .option("header", true)
      .option("delimiter", "\t")
      .csv(input).rdd
      .repartition(partitions).cache()
    logger.info(s"Reading ${input}... Done!")
    logger.info(s"Number of records: ${geometriesTXT.count()}")

    logger.info("Performing re-projection...")
    val source = params.source()
    val target = params.target()
    val geometriesRaw = geometriesTXT.mapPartitions{ geoms =>
      val geofactory1 = new GeometryFactory()
      val geofactory2 = new GeometryFactory(new PrecisionModel(1000))
      val reader = new WKTReader(geofactory1)

      val ctFactory = new CoordinateTransformFactory()
      val csFactory = new CRSFactory()
      val csName1 = source
      val csName2 = target
      val crs1 = csFactory.createFromName(csName1)
      val crs2 = csFactory.createFromName(csName2)
      val trans = ctFactory.createTransform(crs1, crs2)

      geoms.flatMap{ g =>
        val geom = reader.read(g.getString(0))

        (0 until geom.getNumGeometries).map{ i =>
          geom.getGeometryN(i)
        }.map{ geom =>
          val polygonRaw = geom.asInstanceOf[Polygon]
          val nGeoms = polygonRaw.getNumGeometries
          val rings = polygonRaw.getExteriorRing +:
            (0 until polygonRaw.getNumInteriorRing).map{ i =>
              polygonRaw.getInteriorRingN(i)
            }
          val lstRings = rings.map{ ring =>
            val arrCoords = ring.getCoordinates.zipWithIndex.groupBy(_._1).map{ tuple =>
              val coordSource = tuple._1
              val pSource = new ProjCoordinate(coordSource.x, coordSource.y)
              val pTarget = new ProjCoordinate()
              trans.transform(pSource, pTarget)
              val coordTarget = new Coordinate(pTarget.x, pTarget.y)

              val index = tuple._2.map(_._2).sorted.head
              (index, coordTarget)
            }.toList.sortBy(_._1).map(_._2).toArray
            if(arrCoords.length < 3){
              Array.empty[Coordinate]
            } else {
              arrCoords :+ arrCoords.head
            }
          }
          if(lstRings.length > 1){
            val outer = geofactory2.createLinearRing(lstRings.head)
            val inners = lstRings.tail.map(geofactory2.createLinearRing).toArray
            geofactory2.createPolygon(outer, inners)
          } else {
            val outer = geofactory2.createLinearRing(lstRings.head)
            geofactory2.createPolygon(outer)
          }
        }
      }
    }.cache()
    logger.info("Performing re-projection... Done!")
    logger.info(s"Number of records: ${geometriesRaw.count()}")

    val output = params.output()
    logger.info(s"Saving to ${output}...")
    if(partitions < 2){
      val f = new java.io.PrintWriter(output)
      val wkt = geometriesRaw.collect().map{ polygon =>
        s"${polygon.toText()}\n"
      }.mkString("")
      f.write(wkt)
      f.close()
    } else {
      geometriesRaw
        .map(_.toText).toDF().write
        .mode(SaveMode.Overwrite)
        .option("header", false)
        .option("delimiter", "\t")
        .format("csv")
        .save(output)
    }
    logger.info(s"Saving to ${output}... Done!")

    logger.info("Closing session...")
    spark.close()
    logger.info("Closing session... Done!")
  }
}

class TranslateConf(args: Seq[String]) extends ScallopConf(args) {
  val input: ScallopOption[String] = opt[String]  (required = true)
  val output: ScallopOption[String] = opt[String]  (required = true)
  val source: ScallopOption[String] = opt[String]  (default = Some("EPSG:4326"))
  val target: ScallopOption[String] = opt[String]  (default = Some("EPSG:3857"))
  val partitions: ScallopOption[Int] = opt[Int] (default = Some(960))

  verify()
}
