/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors

import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory, Coordinate, Point}
import org.apache.log4j.{Level, Logger}

object KMeansFinder {

  case class Cell(id: Int, point: Point, edges: Int)
  case class Centroid(point: Point, id: Int)
  case class Data(wkt: String, cell_id: Int, class_id: Int, dist: Double, nedges: Long,
    cumulative: Long = 0L) {
    override def toString = s"$wkt\t$cell_id\t$class_id\t$dist\t$nedges\t$cumulative\n"
  }

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    val geofactory = new GeometryFactory(new PrecisionModel(1e8))

    val conf = new SparkConf().setAppName("KMeansExample")
    val sc = new SparkContext(conf)

    val input = args(0)
    val numClusters = args(1).toInt
    val numIterations = args(2).toInt

    val buffer = scala.io.Source
      .fromFile("/home/acald013/RIDIR/Datasets/DCEL/filter.wkt")
    val toFilter = buffer.getLines.map{ line =>
      val arr = line.split("\t")
      arr(1).toInt
    }.toSet
    println(s"Cell to filter: ${toFilter.size}")
    buffer.close

    // Load the data...
    val cells = sc.textFile(input).map{ line =>
      val arr = line.split("\t")
      val i = arr(2).toInt
      val x = arr(3).toDouble
      val y = arr(4).toDouble
      val n = arr(5).toInt
      val p = geofactory.createPoint(new Coordinate(x, y))
      Cell(i, p, n)
    }.filter(cell => !toFilter.contains(cell.id))

    val tedges = cells.map(_.edges).reduce(_ + _)
    println(s"# of edges in dataset: $tedges [${tedges/numClusters}]")

    // Parse the data...
    val parsedData = cells.map{ cell =>
      val x = cell.point.getX
      val y = cell.point.getY
      Vectors.dense(Array(x, y))
    }.cache()

    // Cluster the data into two classes using KMeans
    val model = KMeans.train(parsedData, numClusters, numIterations)

    // Parse centroids...
    val centroids = model.clusterCenters.map{ center =>
      val x = center.toArray(0)
      val y = center.toArray(1)
      geofactory.createPoint(new Coordinate(x, y))
    }.zipWithIndex.map{ case(p, i) => Centroid(p, i) }
    centroids foreach println

    // Measure distance...
    val classes = cells.map{ cell =>
      val dist_and_class = centroids.map{ centroid =>
        (centroid.point.distance(cell.point), centroid.id)
      }.minBy(_._1)
      Data(cell.point.toText, cell.id, dist_and_class._2, dist_and_class._1, cell.edges)
    }

    // Save classes...
    (0 until numClusters).foreach{ c =>
      val sample = classes.filter(_.class_id == c).collect
        .sortBy(s => (s.dist, -s.nedges))
      val cumulative = sample.scanLeft(0L)( (a, b) => a + b.nedges)
      println(s"# of edges per class $c: ${sample.map(_.nedges).reduce(_ + _)}")
      val data = sample.zip(cumulative).map{ case(data, cumulative) =>
        data.copy(cumulative = cumulative)
      }
      val f = new java.io.PrintWriter(s"/tmp/edgesClass${c}.wkt")
      f.write(data.mkString(""))
      f.close
    }

    sc.stop()
  }
}
// scalastyle:on println
