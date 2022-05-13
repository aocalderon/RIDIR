
/**
 *   Copyright 2016 www.alaraph.com
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

/*
 * This is my solution to problem: https://www.hackerrank.com/challenges/range-minimum-query
 * mauro@alaraph.com
 */

package edu.ucr.dblab.sdcel

object SegmentTree2 {

  case class Range(l: Int, r: Int) {
    require (l <= r)

    def intersect(other: Range): Option[Range] = {
      if (this.r < other.l || this.l > other.r){
        None
      } else {
        Some( Range( math.max(this.l, other.l), math.min(this.r, other.r) ) )
      }
    }

    final def &(other: Range): Option[Range] = intersect(other)

    def +(other: Range): Option[Range] = {
      if ( (this & other) != None || ( this.r == other.l - 1 || other.r == this.l - 1 ) ){
        Some( Range( math.min(this.l, other.l), math.max(this.r, other.r) ) )
      } else {
        None
      }
    }

    def >=(other: Range): Boolean = this.l <= other.l && other.r <= this.r

    def  >(other: Range): Boolean = this >= other && this != other 

    def  <(other: Range): Boolean = other > this
    
    def <=(other: Range): Boolean = other >= this
    
    def len = r - l

    override def toString: String = s"(${l}, ${r})"
  }
    
  abstract class SegmentTree[V](val range: Range, val v: V) {
    def eval(rg: Range): V
    final def eval(l: Int, r: Int): V = eval(Range(l, r))
  }

  class Leaf[V](i: Int, v: V) extends SegmentTree(Range(i, i), v) {
    override def toString: String = s"(Range = ${range}, Value = ${v})"
    override def eval(range: Range) = {
      require(range == this.range)
      v
    }
  }

  class Node[V](rg: Range, v: V, val fun: (V,V) => V,
    val left:  SegmentTree[V],
    val right: SegmentTree[V]) extends SegmentTree(rg, v) {

    override def toString: String = s"(Range = ${rg}, Value = ${v})"

    override def eval(rg: Range) = {
      require(this.rg >= rg)
      if (rg == this.rg) v
      else ((left.range intersect rg, right.range intersect rg): @unchecked) match {
          case (None, Some(r)) => right eval r
          case (Some(l), None) => left eval l
          case (Some(l), Some(r)) => fun(left eval l, right eval r)
      }
    }
  }

  object SegmentTree {
    def build[X, V](data: Vector[X], fun: (V,V) => V, xToV: (X) => V): SegmentTree[V] = {
      def buildAcc(l: Int, r: Int): SegmentTree[V] =
        if (l == r)
          new Leaf(l, xToV(data(l)))
        else {
          val n = (l + r) / 2
          val left = buildAcc(l, n)
          val right = buildAcc(n + 1, r)
          new Node(Range(l, r), fun(left.v, right.v), fun, left, right)
        }
      buildAcc(0, data.length - 1)
    }
  }

  import edu.ucr.dblab.sdcel.geometries.Half_edge
  import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory, Coordinate}

  case class Value(collect: List[Half_edge]) {
    override def toString = s"(collect: ${collect.map{_.wkt}.mkString(" ")})"
  }
  
  def xToValue(i: Half_edge) = Value(List(i))
  def fun(v1: Value, v2: Value): Value = Value(v1.collect ++ v2.collect)
      
  import scala.io.StdIn.readLine
  def readDoubles: Vector[Double] = readLine.split(' ').map(_.toDouble).toVector
  def readInts: Vector[Int] = readLine.split(' ').map(_.toInt).toVector

  def main(args: Array[String]): Unit = {
    val model = new PrecisionModel(1000)
    val geofactory = new GeometryFactory(model)
    val data_prime = List(0.1, 0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1)
    val data = data_prime.zip(data_prime.tail).map{ case(a, b) =>
      val l = geofactory.createLineString(Array(new Coordinate(a,a), new Coordinate(b,b)))
      Half_edge(l)
    }.toVector
    val sTree = SegmentTree.build[Half_edge, Value](data, fun, xToValue)
    println(sTree.eval(1, 5))
    println(sTree.eval(3, 5))
    println(sTree.eval(0, 4))
    println(sTree.eval(7, 8))
  }
}
