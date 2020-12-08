package edu.ucr.dblab.tests

import com.vividsolutions.jts.geom.{Geometry, Polygon, Coordinate}
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.enums.GridType
import org.slf4j.{Logger, LoggerFactory}
import edu.ucr.dblab.sdcel.PartitionReader.readQuadtree
import edu.ucr.dblab.sdcel.DCELBuilder2.save
import edu.ucr.dblab.sdcel.quadtree._

import org.scalatest.funsuite.AnyFunSuite

class PolygonDifferTest extends AnyFunSuite {
  val model = new PrecisionModel(1000)
  implicit val geofactory = new GeometryFactory(model)

  val A = Array((0,0),(5,0),(5,2),(5,4),(3,4),(0,4),(0,0))
  val B = Array((3,4),(5,4),(5,8),(5,10),(3,10),(3,4))
  val C = Array((5,2),(10,2),(10,8),(5,8),(5,4),(5,2))
  val polygons = List(A,B,C).zipWithIndex.map{ case(cs, id) =>
    val coords = cs.map{case(x,y) => new Coordinate(x,y)}
    val poly = geofactory.createPolygon(coords)
    poly.setUserData(id)
    poly
  }

  test("The list should have 3 element.") {
    assert(polygons.size == 3)
  }

}
