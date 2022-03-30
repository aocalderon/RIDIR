package edu.ucr.dblab.sdcel


import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.TaskContext

import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.slf4j.{Logger, LoggerFactory}
import ch.cern.sparkmeasure.TaskMetrics

import edu.ucr.dblab.sdcel.cells.EmptyCellManager2.{EmptyCell, getEmptyCells, runEmptyCells}
import edu.ucr.dblab.sdcel.PartitionReader.{readQuadtree, readEdges}
import edu.ucr.dblab.sdcel.DCELOverlay2.overlay
import edu.ucr.dblab.sdcel.Utils.{Tick, Settings, save, log, log2, logger}
import edu.ucr.dblab.sdcel.LocalDCEL.createLocalDCELs
import edu.ucr.dblab.sdcel.SingleLabelChecker.checkSingleLabel

object SDCEL2 {
  def main(args: Array[String]) = {
    implicit val now = Tick(System.currentTimeMillis)
    // Starting session...
    implicit val params = new Params(args)
    implicit val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()
    import spark.implicits._
    implicit val conf = spark.sparkContext.getConf
    val appId = conf.get("spark.app.id")
    val qtag = params.qtag()
    implicit val settings = Settings(
      tolerance = params.tolerance(),
      debug = params.debug(),
      local = params.local(),
      persistance = params.persistance() match {
        case 0 => StorageLevel.NONE
        case 1 => StorageLevel.MEMORY_ONLY
        case 2 => StorageLevel.MEMORY_ONLY_SER
        case 3 => StorageLevel.MEMORY_ONLY_2
        case 4 => StorageLevel.MEMORY_ONLY_SER_2
      },
      appId = appId
    )
    val command = System.getProperty("sun.java.command")
    log2(s"COMMAND|$command")
    log(s"INFO|tolerance=${settings.tolerance}")
    implicit val model = new PrecisionModel(settings.scale)
    implicit val geofactory = new GeometryFactory(model)

    // Reading the quadtree for partitioning...
    implicit val (quadtree, cells) = readQuadtree[Int](params.quadtree(), params.boundary())
    log2(s"TIME|start|$qtag")

    // Reading data...
    val edgesRDDA = readEdges(params.input1(), quadtree, "A")
    val emptiesA = getEmptyCells(edgesRDDA, cells, "A")
    val edgesRDDB = readEdges(params.input2(), quadtree, "B")
    val emptiesB = getEmptyCells(edgesRDDB, cells, "B")
    log2(s"TIME|read|$qtag")

    // Creating local dcel layer A...
    val ldcelA0 = createLocalDCELs(edgesRDDA, cells)
    val (ldcelA, ma) = runEmptyCells(ldcelA0, quadtree, cells, emptiesA, "A")
    log2(s"TIME|layer1|$qtag")

    // Creating local dcel layer B...
    val ldcelB0 = createLocalDCELs(edgesRDDB, cells, "B")
    val (ldcelB, mb) = runEmptyCells(ldcelB0, quadtree, cells, emptiesB, "B")
    log2(s"TIME|layer2|$qtag")

    if(params.overlay()){
      // Overlay local dcels...
      val sdcel = overlay(ldcelA, ma, ldcelB, mb)
      sdcel.count
      log2(s"TIME|overlay|$qtag")

      if(params.debug()){
        save("/tmp/edgesFO.wkt"){
          sdcel.map{ case(l,w) =>
            s"${w.toText}\t$l\t${w.getUserData}\n"
          }.collect
        }
        log2(s"TIME|saveO|$qtag")
      }
    }
    /*
    val faces0 = sdcel.mapPartitionsWithIndex{ (pid, it) =>
      val partitionId = 39
      val hedges = it.toList
      val faces = hedges.map{hedge => Face(hedge._1, hedge._2)}
      if(pid == partitionId){
        println("Test!!!")
        faces.groupBy(_.label)
          .mapValues{ f =>
            val polys = f.sortBy(_.outerArea).reverse
            val outer = polys.head
            outer.inners = polys.tail.toVector
            val wkt = outer.getGeometry.toText
            wkt
          }//.foreach{println}
      }
      faces.groupBy(_.label)
        .mapValues{ f =>
          val polys = f.sortBy(_.outerArea).reverse
          val outer = polys.head
          outer.inners = polys.tail.toVector
          outer
        }.map(_._2).toIterator
      //it
    }.cache
    //save("/tmp/edgesFaces.wkt"){
    //  faces0.map{f => s"${f.getGeometry.toText}\t${f.label}\n"}.collect
    //}

    // Running overlay operations...
    val faces1 = faces0.flatMap{ face =>
      val boundary = face.getGeometry
      val label = face.label
      val polys = if(boundary.getGeometryType == "Polygon"){
        val f = FaceViz(boundary.asInstanceOf[Polygon], label)
        List(f)
      } else {
        val n = boundary.getNumGeometries - 1
        (0 to n).map{ i => 
          val p = boundary.getGeometryN(i).asInstanceOf[Polygon]
          FaceViz(p, label)
        }
      }
      polys
      //flatMap(p => FaceViz(p, label))
      //FaceViz(poly, label)
    }

    /*
    val faces = sdcel_prime.map{ case(hedge, tag) =>
      val boundary = hedge.getPolygon
      FaceViz(boundary, tag)
    }
     */

    val output_path = params.output()
    overlapOp(faces1, intersection, s"${output_path}/edgesInt")
    overlapOp(faces1, difference,   s"${output_path}/edgesDif")
    overlapOp(faces1, union,        s"${output_path}/edgesUni")
    overlapOp(faces1, differenceA,  s"${output_path}/edgesDiA")
    overlapOp(faces1, differenceB,  s"${output_path}/edgesDiB")
     */

    log2(s"TIME|end|$qtag")
    spark.close
  }
}
