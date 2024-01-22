package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel}
import edu.ucr.dblab.sdcel.DCELPartitioner2.read2
import edu.ucr.dblab.sdcel.PartitionReader.readQuadtree
import edu.ucr.dblab.sdcel.Utils.{Settings, Tick, getSettings, log2, printParams, save}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator

object SDCELLocalRunner {
  def main(args: Array[String]): Unit = {
    implicit val now: Tick = Tick(System.currentTimeMillis)
    implicit val params: Params = new Params(args)
    implicit val spark: SparkSession = SparkSession.builder()
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .master(params.master())
      .getOrCreate()
    val qtag = params.qtag()
    implicit val S: Settings = getSettings

    printParams
    implicit val model: PrecisionModel = new PrecisionModel(S.scale)
    implicit val geofactory: GeometryFactory = new GeometryFactory(model)

    val edgesRDDA = read2(params.input1())
    val edgesRDDB = read2(params.input2())
    if(S.debug){
      save("/tmp/edgesRDDA.wkt") {
        edgesRDDA.rawSpatialRDD.rdd.map { linestring =>
          val wkt = linestring.toText
          s"$wkt\n"
        }.collect
      }
      save("/tmp/edgesRDDB.wkt") {
        edgesRDDB.rawSpatialRDD.rdd.map { linestring =>
          val wkt = linestring.toText
          s"$wkt\n"
        }.collect
      }
    }

    // Reading the quadtree for partitioning...
    implicit val (quadtree, cells) = readQuadtree(params.quadtree(), params.boundary())
    /*
    val partitioner = new QuadTreePartitioner()
    edgesRDDA.spatialPartitioning(partitioner)
    val edgesA = edgesRDDA.spatialPartitionedRDD.rdd
    edgesRDDB.spatialPartitioning(partitioner)
    val edgesB = edgesRDDB.spatialPartitionedRDD.rdd
    */

    log2(s"TIME|start|$qtag")

    if(S.debug){
      save("/tmp/edgesQLocal.wkt"){
        cells.map{ case(_, cell) =>
          val wkt = geofactory.toGeometry(cell.mbr.getEnvelopeInternal).toText
          val  id = cell.id

          s"$wkt\t$id\n"
        }.toList
      }
    }

    spark.close
  }
}
