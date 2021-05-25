package edu.ucr.dblab.sdcel

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.SpatialRDD

import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory, LineString}

import edu.ucr.dblab.sdcel.quadtree.{StandardQuadTree, QuadRectangle, QuadTreePartitioner}

import DCELPartitioner2.{logger, read}
import Utils._

object DCELPartitionerTester {
  def main(args: Array[String]) = {
    // Starting session...
    logger.info("Starting session...")
    implicit val spark = SparkSession.builder()
        .config("spark.serializer",classOf[KryoSerializer].getName)
        .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
        .getOrCreate()
    import spark.implicits._
    val params = new Params(args)

    implicit val settings = Settings(
      tolerance = params.tolerance(),
      debug = params.debug(),
      local = params.local(),
      appId = spark.sparkContext.applicationId
    )
    val command = System.getProperty("sun.java.command")
    logger.info(command)

    logger.info(s"Scale: ${settings.scale}")
    val model = new PrecisionModel(settings.scale)
    implicit val geofactory = new GeometryFactory(model)
    logger.info("Starting session... Done!")
    
    // Reading data...
    logger.info("Reading data...")
    val edgesRDDA = read(params.input1())
    val nEdgesRDDA = edgesRDDA.getRawSpatialRDD.count()
    logger.info(s"# of Edges:\t$nEdgesRDDA")
    val edgesRDDB = read(params.input2())
    val nEdgesRDDB = edgesRDDB.getRawSpatialRDD.count()
    logger.info(s"# of Edges:\t$nEdgesRDDB")
    val edgesRDD = edgesRDDA.getRawSpatialRDD.rdd union edgesRDDB.getRawSpatialRDD.rdd
    val nEdgesRDD = nEdgesRDDA + nEdgesRDDB
    
    val boundary = edgesRDDA.boundary
    boundary.expandToInclude(edgesRDDB.boundary)
    logger.info("Reading data... Done!")

    // Partitioning edges...
    def getAvgPerPartition(entrie: Int): (Int, Double, Int, Double) = {
      val definition = new QuadRectangle(boundary)
      val maxentries = entrie
      val maxlevel   = params.maxlevel()
      val fraction   = params.fraction()
      val quadtree = new StandardQuadTree[LineString](definition, 0, maxentries, maxlevel)
      val samples = edgesRDD.sample(false, fraction, 42).collect()
      samples.foreach{ edge =>
        quadtree.insert(new QuadRectangle(edge.getEnvelopeInternal), edge)
      }
      quadtree.assignPartitionIds
      quadtree.assignPartitionLineage
      val partitioner = new QuadTreePartitioner(quadtree)
      val spatialRDD = new SpatialRDD[LineString]()
      spatialRDD.setRawSpatialRDD(edgesRDD)
      spatialRDD.analyze(boundary, nEdgesRDD.toInt)
      spatialRDD.spatialPartitioning(partitioner)
      val sizes = spatialRDD.spatialPartitionedRDD.rdd
        .mapPartitionsWithIndex{ (index, it) =>
          val n = it.length
          Iterator(n)
        }.collect.toList
      val len = sizes.length
      val avg = mean(sizes)
      val max = sizes.max
      val sd  = stdDev(sizes)
      (len, avg, max, sd)
    }

    val entries = 3050 to 5000 by 50
    for(entrie <- entries){
      val (len, avg, max, sd) = getAvgPerPartition(entrie)
      logger.info(s"INFO|len=${len}\tentires=${entrie}\tavg=${avg}\tmax=${max}\tsd=${sd}")
    }
    logger.info("Partitioning edges... Done!")

    spark.close
    
  }
}
