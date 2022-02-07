package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geom.{Polygon, LineString, Point}
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.TaskContext

import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.slf4j.{Logger, LoggerFactory}
import ch.cern.sparkmeasure.TaskMetrics

import edu.ucr.dblab.sdcel.quadtree._
import edu.ucr.dblab.sdcel.geometries._
import edu.ucr.dblab.sdcel.cells.EmptyCellManager2._

import PartitionReader._
import DCELBuilder2.getLDCELs
import DCELMerger2.{merge2, merge3, merge4}
import DCELOverlay2._

import Utils._
import LocalDCEL.createLocalDCELs
import SingleLabelChecker.checkSingleLabel

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
    val conf = spark.sparkContext.getConf
    val appId = conf.get("spark.app.id")
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

    val model = new PrecisionModel(settings.scale)
    implicit val geofactory = new GeometryFactory(model)

    val (quadtree, cells) = readQuadtree[Int](params.quadtree(), params.boundary())

    val qtag = params.qtag()
    if(params.debug()){
      log(s"INFO|scale=${settings.scale}")
      save{s"/tmp/edgesCells_${qtag}.wkt"}{
        cells.values.map{ cell =>
          val wkt = envelope2polygon(cell.mbr.getEnvelopeInternal).toText
          val id = cell.id
          val lineage = cell.lineage
          s"$wkt\t$id\t$lineage\n"
        }.toList
      }
    }
    log(s"INFO|npartitions=${cells.size}")
    log2("TIME|start")

    // Reading data...
    val edgesRDDA = readEdges(params.input1(), quadtree, "A").cache
    val nEdgesRDDA = edgesRDDA.count()
    log(s"INFO|nEdgesA=${nEdgesRDDA}")

    val edgesRDDB = readEdges(params.input2(), quadtree, "B").cache
    val nEdgesRDDB = edgesRDDB.count()
    log(s"INFO|nEdgesB=${nEdgesRDDB}")
    log2("TIME|read")

    // Creating local dcel layer A...
    val ldcelA0 = createLocalDCELs(edgesRDDA, cells)

    /*
    save("/tmp/edgesLA.wkt"){
      ldcelA0.flatMap{ case(h,l,e,p) =>
        h.getNexts.map(n => s"$n\t$l\n")
      }.collect
    }
     */

    /*
    save("/tmp/edgesFA.wkt"){
      ldcelA0.map{ hedge => s"${hedge._1.getPolygon}\t${hedge._2}\n" }.collect
    }
     */

    val ldcelA = runEmptyCells(ldcelA0, quadtree, cells).cache
    save("/tmp/edgesFAC.wkt"){
      ldcelA.map{ hedge => s"${hedge._1.getPolygon}\t${hedge._2}\t${hedge._1.data.isHole}\n" }.collect
    }

    // Creating local dcel layer B...
    val ldcelB0 = createLocalDCELs(edgesRDDB, cells)

    /*
    save("/tmp/edgesLB.wkt"){
      ldcelB0.filter(_._4.getNumPoints == 0).map{ case(h,l,e,p) =>
        s"${h}\t$l\n"
      }.collect
    }
     */

    /*
    save("/tmp/edgesFB.wkt"){
      ldcelB0.map{ hedge => s"${hedge._1.getPolygon}\t${hedge._2}\n" }.collect
    }
    */

    val ldcelB = runEmptyCells(ldcelB0, quadtree, cells).cache
    save("/tmp/edgesFBC.wkt"){
      ldcelB.map{ hedge => s"${hedge._1.getPolygon}\t${hedge._2}\t${hedge._1.data.isHole}\n" }.collect
    }

    // Overlay local dcels...
    
    val sdcel = ldcelB
      .zipPartitions(ldcelB, preservesPartitioning=true){ (iterA, iterB) =>
        val pid = TaskContext.getPartitionId
        val partitionId = 16

        //val A = iterA.map{_._1.getNexts}.flatten.toList
        //val B = iterB.map{_._1.getNexts}.flatten.toList
        val A = iterA.toList
        val B = iterB.toList

        val hedges = merge4(A, B, cells)

        hedges.toIterator
      }//.filter(_._1.checkValidity)
      .cache
    val nSDcel = sdcel.count()
    logger.info("Done!")
    save("/tmp/edgesFC.wkt"){
      sdcel.map{ case(hedge, label, e) => s"${hedge.getPolygon}\t${label}\n" }.collect
    }
    

    val sdcel2 = overlay4(sdcel.map{case(h,l,e)=> (h,l)}).cache
    
    save("/tmp/edgesFE.wkt"){
      val ffinal = sdcel2.map{ case(h,l) =>

        s"${h.wkt}\t$l\n"
      }.collect
      ffinal
    }
    

    /*
    save("/tmp/edgesFE.wkt"){
      val ffinal = mergeSegs(sdcel2).map{ case(l,w) =>

        s"${w.toText}\t$l\t${w.getUserData}\n"
      }.collect
      ffinal
    }
     */  

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


    logger.info("Done!")
    spark.close
  }
}
