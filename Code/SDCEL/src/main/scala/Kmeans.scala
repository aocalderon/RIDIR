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

// scalastyle:off println

import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
// $example off$

import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory, Coordinate, Point}
import org.apache.log4j.{Level, Logger}

object KMeansExample {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    val geofactory = new GeometryFactory(new PrecisionModel(1e8))

    val conf = new SparkConf().setAppName("KMeansExample")
    val sc = new SparkContext(conf)

    // $example on$
    // Load and parse the data
    val input = args(0)
    val data = sc.textFile(input)
    val parsedData = data.map{ line =>
      val arr = line.split("\t")
      val x = arr(3).toDouble
      val y = arr(4).toDouble
      Vectors.dense(Array(x, y))
    }.cache()

    // Cluster the data into two classes using KMeans
    val numClusters = args(1).toInt
    val numIterations = args(2).toInt
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    case class Centroid(point: Point, id: Int)
    val centroids = clusters.clusterCenters.map{ center =>
      val x = center.toArray(0)
      val y = center.toArray(1)
      geofactory.createPoint(new Coordinate(x, y))
    }.zipWithIndex.map{ case(p, i) => Centroid(p, i) }

    case class Cell(id: Int, point: Point, edges: Int)
    val cells = data.map{ line =>
      val arr = line.split("\t")
      val i = arr(2).toInt
      val x = arr(3).toDouble
      val y = arr(4).toDouble
      val n = arr(5).toInt
      val p = geofactory.createPoint(new Coordinate(x, y))
      Cell(i, p, n)
    }

    val classes = cells.map{ cell =>
      val dist_and_class = centroids.map{ centroid =>
        (centroid.point.distance(cell.point), centroid.id)
      }.minBy(_._1)
      (cell.id, dist_and_class._2, dist_and_class._1, cell.edges)
    }

    def saveClass(c: Int): Unit = {
      val f = new java.io.PrintWriter(s"/tmp/kmeans${c}.tsv")
      val content = classes.filter(_._2 == c)
        .sortBy{ case(i,c,d,n) => (d, n) }.map{ case(i,c,d,n) =>
          s"$i\t$c\t$d\t$n\n"
        }.collect
      f.write(content.mkString(""))
      f.close
    }

    saveClass(0)

    sc.stop()
  }
}
// scalastyle:on println
