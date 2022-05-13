import collection.mutable.Stack
import org.scalatest._
import flatspec._
import matchers._

import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory, Coordinate}
import edu.ucr.dblab.debug.BSTreeTest.generateHedges
import edu.ucr.dblab.sdcel.geometries.{StatusKey, Half_edge}

class SweeplineSpec extends AnyFlatSpec with should.Matchers {

  implicit val geofactory = new GeometryFactory(new PrecisionModel(1000.0))

  "A Stack" should "pop values in last-in-first-out order" in {
    val stack = new Stack[Int]
    stack.push(1)
    stack.push(2)
    stack.pop() should be (2)
    stack.pop() should be (1)
  }

  it should "throw NoSuchElementException if an empty stack is popped" in {
    val emptyStack = new Stack[Int]
    a [NoSuchElementException] should be thrownBy {
      emptyStack.pop()
    } 
  }

  val hedges = generateHedges

  val A = hedges.filter(_.tag == "A").head
  val B = hedges.filter(_.tag == "B").head
  val C = hedges.filter(_.tag == "C").head
  val D = hedges.filter(_.tag == "D").head
  val E = hedges.filter(_.tag == "E").head
  val F = hedges.filter(_.tag == "F").head
  val G = hedges.filter(_.tag == "G").head
  val I = hedges.filter(_.tag == "I").head
  val J = hedges.filter(_.tag == "J").head
  val K = hedges.filter(_.tag == "K").head

  val a1 = StatusKey(A, A.left)
  val b = StatusKey(B, B.left)
  val c = StatusKey(C, C.left)
  val d = StatusKey(D, D.left)
  val e = StatusKey(E, E.left)
  val f = StatusKey(F, F.left)
  val g = StatusKey(G, G.left)
  val i = StatusKey(I, I.left)
  val j = StatusKey(J, J.left)
  val k = StatusKey(K, K.left)

  "A" should "be above I" in { StatusKey.isAbove(i.left, a1) should be (-1) }
  it  should "be below F" in { StatusKey.isAbove(f.left, a1) should be  (1) }
  "I" should "be below K" in { StatusKey.isAbove(k.left,  i) should be (-1) } 
  "B" should "be below F" in { StatusKey.isAbove(f.left,  b) should be (-1) }
  "C" should "be below G" in { StatusKey.isAbove(g.left,  c) should be (-1) }
  "D" should "be above D" in { StatusKey.isAbove(e.left,  d) should be  (1) }
  "B" should "be below J" in { StatusKey.isAbove(j.left,  b) should be (-1) }

  "A above I" should "be false" in { StatusKey.above(a1,i) should be (false) }//F
  "I above K" should "be false" in { StatusKey.above(i, k) should be (false) }//F
  "A above F" should "be true " in { StatusKey.above(a1,f) should be  (true) }//V
  "B above F" should "be false" in { StatusKey.above(b, f) should be (false) }//F
  "C above G" should "be false" in { StatusKey.above(c, g) should be (false) }//F
  "D above E" should "be true " in { StatusKey.above(d, e) should be  (true) }//V
  "B above J" should "be false" in { StatusKey.above(b, j) should be (false) }//F

  "A above2 I" should "be false" in { StatusKey.above(a1, i.left, i.right) should be (false) }//F
  "I above2 K" should "be false" in { StatusKey.above(i,  k.left, k.right) should be (false) }//F
  "A above2 F" should "be  true" in { StatusKey.above(a1, f.left, f.right) should be  (true) }//V
  "B above2 F" should "be false" in { StatusKey.above(b,  f.left, f.right) should be (false) }//F
  "C above2 G" should "be false" in { StatusKey.above(c,  g.left, g.right) should be (false) }//F
  "D above2 E" should "be  true" in { StatusKey.above(d,  e.left, e.right) should be  (true) }//V
  "B above2 J" should "be false" in { StatusKey.above(b,  j.left, j.right) should be (false) }//F

}
