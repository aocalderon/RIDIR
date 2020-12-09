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
  val model = new PrecisionModel(10)
  implicit val geofactory = new GeometryFactory(model)

  val Ac = Array((0,0),(5,0),(5,2),(5,4),(3,4),(0,4),(0,0))
  val Bc = Array((3,4),(5,4),(5,8),(5,10),(3,10),(3,4))
  val Cc = Array((5,2),(10,2),(10,8),(5,8),(5,4),(5,2))
  val polygons = List(Ac,Bc,Cc).zipWithIndex.map{ case(cs, id) =>
    val coords = cs.map{ case(x,y) => new Coordinate(x.toDouble,y.toDouble)}
    val poly = geofactory.createPolygon(coords)
    poly.setUserData(id)
    poly
  }

  test("The list should have 3 elements.") {
    assert(polygons.size == 3)
  }

  test("Polygons should intersect."){
    assert{
      polygons(0).intersects(polygons(1)) &&
      polygons(0).intersects(polygons(2)) &&
      polygons(1).intersects(polygons(2)) 
    }
  }

  test("Polygons should not overlap."){
    assert{
      !polygons(0).overlaps(polygons(1)) &&
      !polygons(0).overlaps(polygons(2)) &&
      !polygons(1).overlaps(polygons(2)) 
    }
  }

  test("Polygons B and C overlap A."){
    val A = polygons(0)
    val B = polygons(1).buffer(0.5)
    val C = polygons(2).buffer(0.5)

    assert{
      A.overlaps(B) && A.overlaps(C)
    }
  }

  test("Polygon A relates B is 2********."){
    val A = polygons(0)
    val B = polygons(1).buffer(0.5)

    assert{
      A.relate(B, "2********") 
    }
  }

  test("There should be not overlapping polygons."){
    val A = polygons(0)
    val B = polygons(1)
    val C = polygons(2)
    val ps = List(A,B,C)

    val r = for{
      p1 <- ps
      p2 <- ps
      if p1.relate(p2, "2********") &&
      p1.getUserData.asInstanceOf[Int] < p2.getUserData.asInstanceOf[Int]
    } yield {
      (p1, p2)
    }

    assert{
      r.isEmpty
    }
  }

  test("There should be 2 pairs of polygons which overlaps."){
    val A = polygons(0)
    val B = polygons(1).buffer(0.5)
    B.setUserData(polygons(1).getUserData)
    val C = polygons(2)
    val ps = List(A,B,C)

    val r = for{
      p1 <- ps
      p2 <- ps
      if p1.relate(p2, "2********") &&
      p1.getUserData.asInstanceOf[Int] < p2.getUserData.asInstanceOf[Int]
    } yield {
      (p1, p2)
    }

    assert{
      r.map{ case(a, b) =>
        val aid = a.getUserData
        val bid = b.getUserData
        (aid, bid)
      } == List((0,1), (1,2))
    }
  }
}
