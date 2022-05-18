
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

package edu.ucr.dblab.tests

object SegmentTree2 {

  case class Range(l: Int, r: Int) {
    require (l<=r)
    def intersect(rg: Range):Option[Range] =
      if (this.r < rg.l || this.l > rg.r) None
      else Some(Range(math.max(this.l, rg.l), math.min(this.r, rg.r)))

    final def &(rg:Range) = intersect(rg)

    def +(rg:Range) = if ((this & rg) != None ||
                          (this.r == rg.l - 1 || rg.r == this.l -1))
                        Some(Range(math.min(this.l, rg.l), math.max(this.r, rg.r)))
                      else None

    def >=(rg: Range):Boolean =
      if (this.l <= rg.l && rg.r <= this.r) true
      else false

    def >(rg: Range):Boolean =
      if (this != rg && (this >= rg)) true else false

    def <(rg: Range) = rg > this
    
    def <=(rg: Range) = rg >= this
    
    def len = r - l + 1

    override def toString: String = "(%d, %d)".format(l, r)
  }
    
  abstract class SegmentTree[V](val rg: Range, val v: V) {
    def eval(r: Range): V
    final def eval(l:Int, r:Int):V = eval(Range(l, r))
  }

  class Leaf[V](i: Int, v: V) extends SegmentTree(Range(i, i), v) {
    override def toString: String = "(range=%s, value=%s)".format(rg, v)
    override def eval(rg:Range) = {
      require(rg == this.rg)
      v
    }
  }

  class Node[V](rg: Range,
                v: V,
                val fun: (V,V) => V,
                val left: SegmentTree[V],
                val right: SegmentTree[V]) extends SegmentTree(rg, v) {
    override def toString: String = "(range=%s, value=%s)".format(rg, v)

    override def eval(rg:Range) = {
      require(this.rg >= rg)
      if (rg == this.rg) v
      else ((left.rg intersect rg, right.rg intersect rg): @unchecked) match {
          case (None, Some(r)) => right eval r
          case (Some(l), None) => left eval l
          case (Some(l), Some(r)) => fun(left eval l, right eval r)
      }
    }
  }

  object SegmentTree {
    def build[X,V](data: Vector[X], fun:(V,V)=>V, xToV:(X)=>V): SegmentTree[V] = {
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

  case class Value(min: Int) {
    override def toString = "(min:%d)".format(min)
  }
  
  def intToValue(i: Int) = Value(i)
  def fun(v1: Value, v2: Value): Value = Value(math.max(v1.min, v2.min))
      
  import scala.io.StdIn.readLine
  def readInts: Vector[Int] = readLine.split(' ').map(_.toInt).toVector

  def main(args: Array[String]): Unit = {
    val Vector(nelem, nqueries) = readInts
    val sTree = SegmentTree.build(readInts, fun, intToValue)
    val queries = for (i <- 1 to nqueries)
                  yield readInts
    queries.foreach(x => println(sTree.eval(x(0), x(1)).min))
  }
}
