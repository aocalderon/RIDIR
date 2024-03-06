package sdcel

import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel}
import edu.ucr.dblab.sdcel.DCELOverlay2.{overlay, overlayByLevel, overlayMaster}
import edu.ucr.dblab.sdcel.LocalDCEL.createLocalDCELs
import edu.ucr.dblab.sdcel.Params
import edu.ucr.dblab.sdcel.PartitionReader.{readEdges, readQuadtree}
import edu.ucr.dblab.sdcel.Utils.{Settings, Tick, loadSDCEL, log, log2, save, saveSDCEL}
import edu.ucr.dblab.sdcel.cells.EmptyCellManager2.{EmptyCell, getNonEmptyCells}
import edu.ucr.dblab.sdcel.geometries.Cell
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import edu.ucr.dblab.sdcel.DCELPartitionerByGrids.readGrids

object SDCEL3 {
  def main(args: Array[String]) = {
    // Starting session...
    implicit val now = Tick(System.currentTimeMillis)
    implicit val params = new Params(args)
    implicit val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()
    implicit val conf = spark.sparkContext.getConf
    val appId = conf.get("spark.app.id")
    val qtag = params.qtag()
    implicit val settings = Settings(
      tolerance = params.tolerance(),
      debug = params.debug(),
      local = params.local(),
      ooption = params.ooption(),
      olevel = params.olevel(),
      appId = appId,
      persistance = params.persistance() match {
        case 0 => StorageLevel.NONE
        case 1 => StorageLevel.MEMORY_ONLY
        case 2 => StorageLevel.MEMORY_ONLY_SER
        case 3 => StorageLevel.MEMORY_ONLY_2
        case 4 => StorageLevel.MEMORY_ONLY_SER_2
      }
    )
    val command = System.getProperty("sun.java.command")
    if(settings.debug){
      log(s"INFO|command=$command")
      log(s"INFO|tolerance=${settings.tolerance}")
      log(s"INFO|overlay_option=${settings.ooption}")
      log(s"INFO|overlay_level=${settings.olevel}")
      log(s"INFO|local=${settings.local}")
      log(s"INFO|appId=${settings.appId}")
      log(s"INFO|persistance=${settings.persistance}")
    }
    implicit val model = new PrecisionModel(settings.scale)
    implicit val geofactory = new GeometryFactory(model)

    // Reading the quadtree for partitioning...
    //val (quadtree, cells) = readQuadtree[Int](params.quadtree(), params.boundary())

    implicit val grids: Map[Int, Cell] = readGrids(params.grids())

    log2(s"TIME|start|$qtag")

    // Reading data...
    val (ldcelA, ma, ldcelB, mb) = if(params.loadsdcel()){
      val ldcelA = loadSDCEL(params.input1(), "A")
      if(settings.debug){
        save("/tmp/edgesA.wkt"){
          ldcelA.mapPartitionsWithIndex{ (pid, it) =>
            it.map{ case(h,l,e,p) =>
              val wkt = p.toText
              s"$wkt\t$l\t$pid\n"
            }
          }.collect
        }
      }
      val ldcelB = loadSDCEL(params.input2(), "B")
      if(settings.debug){
        save("/tmp/edgesB.wkt"){
          ldcelB.mapPartitionsWithIndex{ (pid, it) =>
            it.map{ case(h,l,e,p) =>
              val wkt = p.toText
              s"$wkt\t$l\t$pid\n"
            }
          }.collect
        }
      }
      val m = Map.empty[String, EmptyCell]
      
      (ldcelA, m, ldcelB, m)
    } else {
      val edgesRDDA = readEdges(params.input1(), "A")
      val non_emptiesA = getNonEmptyCells(edgesRDDA, "A")
      val edgesRDDB = readEdges(params.input2(), "B")
      val non_emptiesB = getNonEmptyCells(edgesRDDB, "B")
      log2(s"TIME|read|$qtag")

      if(params.debug()){
        save("/tmp/edgesRDDA.wkt"){
          edgesRDDA.map{ edge =>
            val wkt = edge.toText
            s"$wkt\n"
          }.collect
        }
        save("/tmp/edgesRDDB.wkt"){
          edgesRDDB.map{ edge =>
            val wkt = edge.toText
            s"$wkt\n"
          }.collect
        }
      }

      // Creating local dcel layer A...
      val ldcelA = createLocalDCELs(edgesRDDA, "A")
      if(params.debug()){
        save("edgesA.wkt"){
          ldcelA.map{ case(hedge, label, envelope, polygon) =>
            val wkt = hedge.wkt

            s"$wkt\t$label\n"
          }.collect
        }
      }
      //val ma = runEmptyCells(ldcelA, non_emptiesA, "A")
      val mx = Map.empty[String, EmptyCell]
      log2(s"TIME|layer1|$qtag")
      if(params.savesdcel()){
        saveSDCEL(s"${params.input1().split("edgesA")(0)}/ldcelA", ldcelA, mx)
      }

      // Creating local dcel layer B...
      val ldcelB = createLocalDCELs(edgesRDDB, "B")
      if(params.debug()){
        save("edgesB.wkt"){
          ldcelB.map{ case(hedge, label, envelope, polygon) =>
            val wkt = hedge.wkt

            s"$wkt\t$label\n"
          }.collect
        }
      }
      //val mb = runEmptyCells(ldcelB, non_emptiesB, "B")
      log2(s"TIME|layer2|$qtag")
      if(params.savesdcel()){
        saveSDCEL(s"${params.input2().split("edgesB")(0)}/ldcelB", ldcelB, mx)
      }

      (ldcelA, mx, ldcelB, mx)
    }
    
    if(params.overlay()){
      // Overlay local dcels...
      settings.ooption match {
        case 0 => {
          overlay(ldcelA, ma, ldcelB, mb)
          log2(s"TIME|overlay|$qtag")
        }
        case 1 => {
          overlayMaster(ldcelA, ma, ldcelB, mb)
          log2(s"TIME|overlayMaster|$qtag")
        }
        case 2 => {
          overlayByLevel(ldcelA, ma, ldcelB, mb)
          log2(s"TIME|overlayByLevel|$qtag")
        }
      }

      /*
       val faces0 = sdcel.mapPartitionsWithIndex{ (pid, it) =>
       val hedges = it.toList
       val faces = hedges.map{hedge => Face(hedge._1, hedge._2)}
       faces.groupBy(_.label)
       .mapValues{ f =>
       val polys = f.sortBy(_.outerArea).reverse
       val outer = polys.head
       outer.inners = polys.tail.toVector
       outer
       }.map(_._2).toIterator
       }.cache

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
       }

       val output_path = params.output()
       overlapOp(faces1, intersection, s"${output_path}/edgesInt")
       overlapOp(faces1, difference,   s"${output_path}/edgesDif")
       overlapOp(faces1, union,        s"${output_path}/edgesUni")
       overlapOp(faces1, differenceA,  s"${output_path}/edgesDiA")
       overlapOp(faces1, differenceB,  s"${output_path}/edgesDiB")
       */
    }

    log2(s"TIME|end|$qtag")
    spark.close
  }
}
