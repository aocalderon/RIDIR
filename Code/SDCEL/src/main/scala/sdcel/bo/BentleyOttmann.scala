 package edu.ucr.dblab.bo3

import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel}
import com.vividsolutions.jts.geom.{Envelope, Coordinate, LineString, Point}

import org.jgrapht.graph.{SimpleDirectedGraph, DefaultEdge}
import org.jgrapht.Graphs

import scala.collection.JavaConverters._
import scala.collection.mutable.{ListBuffer, ArrayBuffer, HashMap}

import java.util.{PriorityQueue, TreeSet, TreeMap}

import edu.ucr.dblab.sdcel.Utils.{save, logger}
import edu.ucr.dblab.sdcel.geometries.Half_edge

object BentleyOttmann {
  implicit val model: PrecisionModel = new PrecisionModel(1e-3)
  implicit val geofactory: GeometryFactory = new GeometryFactory(model)
  implicit var T: TreeSet[Node]        = new TreeSet[Node]()
  implicit var Q: PriorityQueue[Event] = new PriorityQueue[Event]()
  implicit val G: SimpleDirectedGraph[Coordinate, SegmentEdge] =
    new SimpleDirectedGraph[Coordinate, SegmentEdge](classOf[SegmentEdge])
  var X: ListBuffer[Intersection]      = new ListBuffer[Intersection]()

  /**** Main Class Start ****/
  def sweep_segments(segs1: List[Segment], segs2: List[Segment])
    (implicit settings: Settings, G: SimpleDirectedGraph[Coordinate, SegmentEdge])
      : List[Intersection] = {

    // local declarations...
    val S: List[Segment] = segs1 ++ segs2

    // The X-Structure: Event queue...
    implicit val X_structure: TreeMap[Coordinate, Seq_item] = new TreeMap[Coordinate, Seq_item]()

    implicit val last_node: TreeMap[Segment, Coordinate] = new TreeMap[Segment, Coordinate]()
    val seg_queue: PriorityQueue[Segment] = new PriorityQueue[Segment]( new segmentByXY )

    // Initialization...
    val L = ListBuffer[Segment]()
    val M = ListBuffer[(Segment, Segment)]()
    // Feeding the X-Structure...
    S.foreach{ s =>
      X_structure.put(s.first,  null)
      X_structure.put(s.second, null)

      if(s.source != s.target) { // Ignore zero-length segments...
        val s1 = if(!s.isVertical){ if(!s.isLeftOriented)    { s.reverse } else { s } }
                             else { if(!s.isUpwardsOriented) { s.reverse } else { s } }
        M.append(s1 -> s)
        L.append(s1)
        seg_queue.add(s1)
      }
    }
    val internal: List[Segment] = L.toList
    implicit val original: Map[Segment, Segment] = M.toMap

    // Setting lower and upper sentinels to bound the algorithm...
    val (lower_sentinel, upper_sentinel) = getSentinels
    var p_sweep = lower_sentinel.source

    // Setting the order criteria for Y-Structure
    val cmp = new sweep_cmp()
    cmp.setPosition(p_sweep)

    // The Y-Structure: Sweep line status...
    implicit val Y_structure: TreeMap[Segment, Seq_item] = new TreeMap[Segment, Seq_item](cmp)

    // Adding sentinels...
    val r1 = Y_structure.put(lower_sentinel, null)
    val r2 = Y_structure.put(upper_sentinel, null)
    var next_seg = seg_queue.poll()

    // Just for debugging purposes...
    if( settings.debug ){
      save("/tmp/edgesX.wkt"){
        getAll_X(X_structure).zipWithIndex.map{ case(event_point, index) =>
          val x = event_point.x
          val y = event_point.y

          s"POINT( $x $y )\t$event_point\t$index\n"
        }
      }

      save("/tmp/edgesY.wkt"){
        Y_structure.asScala.map{ case(s, it) =>
          s"${s.wkt}\n"
        }.toList
      }
    }

    // Main sweep loop (LEDA Book pag 745)...
    while( !X_structure.isEmpty ) {
      val event = X_structure.pollFirstEntry()
      p_sweep = event.getKey
      cmp.setPosition(p_sweep)
      G.addVertex(p_sweep)
      val v: Coordinate = p_sweep    

      // Handle passing and ending segments...
      var sit = if( event.getValue == null ){ 
        lookup( createSegment(p_sweep, p_sweep) )
      } else {
        event.getValue
      }

      if( sit != null ){
        // Determine passing and ending segments...
        val (sit_succ, sit_pred, sit_pred_succ, sit_first) =
          determineSegments(sit, event.getValue, v, p_sweep)
        // Reverse order of passing segments...
        reverseOrder(sit_succ, sit_pred, sit_pred_succ, sit_first)
      }

      // Insert starting segments...


      // Compute new intersections and update X_structure...

    }

    List.empty[Intersection]
  }
  /***** Main Class End *****/

  /* Determine passing and ending segments. LEDA book pag 748. */
  def determineSegments(sit_prime: Seq_item, event: Seq_item, v: Coordinate, p_sweep: Coordinate)
    (implicit
      settings:    Settings,
      Y_structure: TreeMap[Segment, Seq_item],
      original:    Map[Segment, Segment],
      last_node:   TreeMap[Segment, Coordinate],
      G:           SimpleDirectedGraph[Coordinate, SegmentEdge]
    ): (Seq_item, Seq_item, Seq_item, Seq_item) = {

    // walk up...
    var sit_succ: Seq_item = null
    var sit_pred: Seq_item = null
    var sit_pred_succ: Seq_item = null
    var sit_first: Seq_item = null

    var sit: Seq_item = sit_prime
    while( inf(sit) == event || inf(sit) == succ(sit) ) { sit = succ(sit) }
    sit_succ = succ(sit)
    val sit_last: Seq_item = sit
    if( settings.use_optimization ) { /* optimization, part 1  */ }

    // walk down
    var overlapping: Boolean = false
    do {
      overlapping = false
      val s = key(sit)
      val w = last_node.get(s)
      if( !settings.embed && s.source == original(s).source ){
        new_edge(w, v, s)
      } else {
        new_edge(v, w, s)
      }
      if( identical(p_sweep, s.target) ) { // ending segment...
        val it = pred(sit)
        if( inf(it) == sit ) {
          overlapping = true
          change_inf( it, inf(sit) )
        }
        del_item(sit)
        sit = it
      } else { // passing segment...
        if( inf(sit) != succ(sit) ) change_inf( sit, null )
        last_node.replace(s, v)
        sit = pred(sit)
      }
    } while ( inf(sit) == event || overlapping || inf(sit) == succ(sit) )

    sit_pred = sit
    sit_first = succ(sit_pred)
    sit_pred_succ = sit_first

    (sit_succ, sit_pred, sit_pred_succ, sit_first)
  }

  /* Reverse order of passing segmentes. LEDA Book pag 749 */
  def reverseOrder(sit_succ: Seq_item, sit_pred: Seq_item, sit_pred_succ: Seq_item,
    sit_first: Seq_item)
    (implicit
      settings:    Settings,
      Y_structure: TreeMap[Segment, Seq_item]
    ): Seq_item = {

    var sit = sit_first

    // reverse subsequences of overlapping segments (if existing)...
    while( sit != sit_succ ){
      val sub_first = sit
      var sub_last  = sub_first
      while( inf(sub_last) == succ(sub_last) ) {
        sub_last = succ(sub_last)
      }
      if( sub_last !=  sub_first ) {
        reverse_items(sub_first, sub_last)
      }
      sit = succ(sub_first)
    }

    // reverse the entire bundle...
    if( sit_first != sit_succ ){
      reverse_items( succ(sit_pred), pred(sit_succ) )
    }

    sit
  }

  /* Insertion of starting segments. LEDA Book pag 750 */
  def insertStartingSegments(sit_prime: Seq_item, p_sweep: Coordinate, next_seg: Segment)
    (implicit Y_structure: TreeMap[Segment, Seq_item]): Unit = {

    var sit = sit_prime
    while( identical(p_sweep, next_seg.source) ) {
      val s_sit = locate(next_seg)
      val p_sit = pred(s_sit)

      val s = key(s_sit)
      if( orientation(s, next_seg.source) == 0 && orientation(s, next_seg.target) == 0 ) {

      } else {

      }

    }
  }

  def identical(p: Coordinate, q: Coordinate): Boolean = p.equals2D(q)

  /**********************************************************************/
  /* START: Sortseq methods (LEDA Book pag 181). Basic Functionality... */
  /*        Extracting just those used and requested in pag 740...      */
  /**********************************************************************/

  // Returns the key of item it...
  def key(it: Seq_item): Segment = it.key.getKeySegment

  // Returns the information of item it (get retuns null if this map contains no mapping for the key)...
  def inf(it: Seq_item)(implicit S: TreeMap[Segment, Seq_item]): Seq_item = S.get( key(it) )

  // Returns the item with key k (null if no such item exits in S)...
  def lookup(k: Segment)(implicit S: TreeMap[Segment, Seq_item]): Seq_item = S.get(k)

  // Associates information i with key k
  // If there is an item [k, j] in S then j is replaced by i,
  //    otherwise a new item [k, i] is added to S.
  // In both cases the item is returned...
  def insert(k: Segment, i: Seq_item)(implicit S: TreeMap[Segment, Seq_item]): Seq_item = {
    if( lookup(k) == null ){
      S.put(k, i)
      i
    } else {
      S.replace(k, i)
    }
  }

  // Returns the item [k', i] in S such that k' is minimal with k' >= k (null if no such item exists)
  def locate(k: Segment)
    (implicit S: TreeMap[Segment, Seq_item]): Seq_item = S.ceilingEntry(k).getValue

  // Equivalent to locate(k)...
  def succ(it: Seq_item)
    (implicit S: TreeMap[Segment, Seq_item]): Seq_item = locate( key(it) )

  // Returns the item [k', i] in S such that k' is maximal with k' <= k (null if no such item exists)
  def pred(it: Seq_item)
    (implicit S: TreeMap[Segment, Seq_item]): Seq_item = S.floorEntry( key(it) ).getValue

  // Returns true if S is empty, false otherwise...
  def empty(implicit S: TreeMap[Segment, Seq_item]): Boolean = S.isEmpty 

  // Returns the item with minimal key (null if S is empty)...
  def min(implicit S: TreeMap[Segment, Seq_item]): Seq_item = S.firstEntry().getValue

  // Removes the item with key k from S (null operation if no such item exists)...
  def del(k: Segment)(implicit S: TreeMap[Segment, Seq_item]): Unit = S.remove( k )

  // Removes the item it from the sequence containing it...
  def del_item(it: Seq_item)(implicit S: TreeMap[Segment, Seq_item]): Unit = del( key(it) )

  // Makes i the information of item it... 
  def change_inf(it: Seq_item, i: Seq_item): Unit = { it.inf = i }

  // If it1 and it2 are items of S with it1 before it2 then reverse_items(it1, it2)
  // reverses the subsequence of S starting at item it1 and ending at item it2...
  // pag 
  def reverse_items(it1: Seq_item, it2: Seq_item)(implicit S: TreeMap[Segment, Seq_item]): Unit = {
    val sub_map = S.subMap( key(it1), key(it2) ).asScala  // get the slice from it1 to it2...
    val keys = sub_map.map(_._1).toList                   // extract the keys...
    val vals = sub_map.map(_._2).toList.reverse           // extract and reverse the values...
    keys.foreach{ k => S.remove(k) }                      // remove items from S...
    val s_prime = keys.zip(vals)                          // match keys and new values...
    s_prime.foreach{ case(k, v) => S.put(k, v) }          // adding new items to S...
  }

  /********************************************************************/
  /* END: Sortseq methods (LEDA Book pag 181). Basic Functionality... */
  /********************************************************************/

  def new_node(p: Coordinate)(implicit settings: Settings): Coordinate = {
    G.addVertex(p)
    p
  }

  def new_edge(v: Coordinate, w: Coordinate, s: Segment)(implicit settings: Settings): SegmentEdge = {
    val se = SegmentEdge(s)
    G.addEdge(v, w, se)
    se
  }

  def getAll_X(X: TreeMap[Coordinate, Seq_item]): List[Coordinate] = {
    def get(X: TreeMap[Coordinate, Seq_item], cursor: Coordinate, R: List[Coordinate]):
        List[Coordinate] = {
      if(cursor == X.lastKey()){
        R :+ cursor
      } else {
        val newR = R :+ cursor
        get(X, X.higherKey(cursor), newR)
      }
    }

    get(X, X.firstKey(), List.empty[Coordinate])
  }

  def createSegment(p1: Coordinate, p2: Coordinate)(implicit geofactory: GeometryFactory): Segment = {
    val line = geofactory.createLineString(Array(p1, p2))
    val hedge = Half_edge(line)
    hedge.id = -3
    Segment(hedge, "*")
  }

  def getSentinels(implicit geofactory: GeometryFactory): (Segment, Segment) = {
    val infinity = Double.MaxValue - 1
    val arr1 = Array( new Coordinate(-infinity, -infinity), new Coordinate(-infinity, infinity) )
    val arr2 = Array( new Coordinate( infinity, -infinity), new Coordinate( infinity, infinity) )
    val l1 = geofactory.createLineString(arr1)
    val l2 = geofactory.createLineString(arr2)
    val h1 = Half_edge(l1)
    h1.id = -1
    val h2 = Half_edge(l2)
    h2.id = -2
    val lower = Segment(h1, "Lower")
    val upper = Segment(h2, "Upper")
    (lower, upper)
  }

  def readSegments(input_data: List[Segment]): Unit = {
    input_data.foreach { s =>
      this.Q.add(Event(s.first,  List(s), 0))
      this.Q.add(Event(s.second, List(s), 1))
    }
  }

  def loadData(implicit geofactory: GeometryFactory): (List[Point], List[Segment]) = {
    val j = geofactory.createPoint(new Coordinate( 0.125, 1.450)); j.setUserData("j")
    val i = geofactory.createPoint(new Coordinate( 6.000, 7.000)); i.setUserData("i")
    val m = geofactory.createPoint(new Coordinate(-0.300, 5.250)); m.setUserData("m")
    val g = geofactory.createPoint(new Coordinate( 5.000, 5.250)); g.setUserData("g")
    val n = geofactory.createPoint(new Coordinate(-0.300, 6.500)); n.setUserData("n")
    val h = geofactory.createPoint(new Coordinate( 5.000, 8.150)); h.setUserData("h")
    val k = geofactory.createPoint(new Coordinate( 1.000, 1.000)); k.setUserData("k")
    val b = geofactory.createPoint(new Coordinate( 1.000, 9.500)); b.setUserData("b")
    val d = geofactory.createPoint(new Coordinate( 2.800, 2.000)); d.setUserData("d")
    val e = geofactory.createPoint(new Coordinate( 3.250, 9.000)); e.setUserData("e")

    val l1 = geofactory.createLineString(Array(j.getCoordinate, i.getCoordinate))
    val l2 = geofactory.createLineString(Array(m.getCoordinate, g.getCoordinate))
    val l3 = geofactory.createLineString(Array(n.getCoordinate, h.getCoordinate))
    val l4 = geofactory.createLineString(Array(k.getCoordinate, b.getCoordinate))
    val l7 = geofactory.createLineString(Array(e.getCoordinate, g.getCoordinate))

    val p1 = new Coordinate(-10, 1.75);val p2 = new Coordinate(10, 1.75)
    val l10 = geofactory.createLineString(Array(p1, p2)) 
    val p3 = new Coordinate(-10, 3.25);val p4 = new Coordinate(10, 3.25)
    val l11 = geofactory.createLineString(Array(p3, p4)) 

    val f = l1.intersection(l2).asInstanceOf[Point];  f.setUserData("f")
    val a = l3.intersection(l4).asInstanceOf[Point];  a.setUserData("a")
    val p = l2.intersection(l4).asInstanceOf[Point];  p.setUserData("p")
    val l = l1.intersection(l10).asInstanceOf[Point]; l.setUserData("l")
    val c = l1.intersection(l11).asInstanceOf[Point]; c.setUserData("c")

    val P = List(a,b,c,d,e,f,g,h,i,j,k,l,m,n,p)

    val l5 = geofactory.createLineString(Array(a.getCoordinate, e.getCoordinate))
    val l6 = geofactory.createLineString(Array(d.getCoordinate, f.getCoordinate))
    val l8 = geofactory.createLineString(Array(l.getCoordinate, c.getCoordinate))
    val l9 = geofactory.createLineString(Array(p.getCoordinate, e.getCoordinate))

    val h1 = Half_edge(l1); h1.id = 1
    val h2 = Half_edge(l2); h2.id = 2
    val h3 = Half_edge(l3); h3.id = 3
    val h4 = Half_edge(l4); h4.id = 4
    val h5 = Half_edge(l5); h5.id = 5
    val h6 = Half_edge(l6); h6.id = 6
    val h7 = Half_edge(l7); h7.id = 7
    val h8 = Half_edge(l8); h8.id = 8
    val h9 = Half_edge(l9); h9.id = 9

    val s1 = Segment(h1, "A")
    val s2 = Segment(h2, "A")
    val s3 = Segment(h3, "A")
    val s4 = Segment(h4, "A")
    val s5 = Segment(h5, "A")
    val s6 = Segment(h6, "A")
    val s7 = Segment(h7, "A")
    val s8 = Segment(h8, "A")
    val s9 = Segment(h9, "A")

    val S = List(s1,s2,s3,s4,s5,s6,s7,s8,s9)

    (P, S)
  }

  def getInternalOriginalMap(segments: List[Segment], seg_queue: PriorityQueue[Segment]):
      (Map[Segment, Segment], List[Segment]) = {

    val L = ListBuffer[Segment]()
    val M = segments.map{ segment =>
      if(!segment.isVertical){
        if(!segment.isLeftOriented){
          val r = segment.reverse
          L.append(r)
          (r, segment)
        } else {
          L.append(segment)
          (segment, segment)
        }
      } else {
        if(!segment.isUpwardsOriented){
          val r = segment.reverse
          L.append(r)
          (r, segment)
        } else {
          L.append(segment)
          (segment, segment)
        }
      }
    }.toMap

    (M, L.toList)
  }

  //////////////////////////////////////// Primitives [Start] ////////////////////////////////////////

  val NO_IDEA = 2

  // Based on https://www.geeksforgeeks.org/orientation-3-ordered-points/
  // To find orientation of ordered triplet (p1, p2, p3). The function returns
  // following values:
  // -1 --> Clockwise
  //  0 --> p, q and r are collinear
  //  1 --> Counterclockwise
  def orientation(p1: Coordinate, p2: Coordinate, p3: Coordinate): Int = {
    // See 10th slides from following link for derivation of the formula...
    // http://www.dcs.gla.ac.uk/~pat/52233/slides/Geometry1x1.pdf
    val value = (p2.y - p1.y) * (p3.x - p2.x) - (p2.x - p1.x) * (p3.y - p2.y)

    value match {
      case x if x <  0 =>  1  //  counterclock wise
      case x if x == 0 =>  0  //  collinear
      case x if x  > 0 => -1  //  clock wise
      case _ => {
        // Happening with Double Overflow (Infinity * Zero)...
        logger.warn("Warning on point - segment orientation...")
        logger.warn(s"value: $value\tp1: $p1\tp2: $p2\tp3: $p3")
        -1
      }
    }
  }

  def orientation(s: Segment, p: Coordinate): Int = {
    orientation(s.h.v1, s.h.v2, p)
  }

  private def sign(x: Double): Int = {
    x match {
      case _ if x <  0 => -1  
      case _ if x == 0 =>  0  
      case _ if x  > 0 =>  1
      case _ => NO_IDEA
    }
  }

  /* See section 10.7 pag 739 at LEDA Book (Mehlhorn et al, 1997)*/
  def intersection_of_lines(s1: Segment, s2: Segment): Option[Coordinate] = {
    if(s1.slope == s2.slope){
      None
    } else if(s1.isDegenerate || s2.isDegenerate){
      None
    } else {
      s1.intersection(s2)
    }
  }

  /* See section 10.7 pag 739 at LEDA Book (Mehlhorn et al, 1997)*/
  def cmp_slopes(s1: Segment, s2: Segment): Int = {
    (s1.slope, s2.slope) match {
      case (None, None) => 0
      case (Some(slope), None) => 0
      case (None, Some(slope)) => 0
      case (Some(ss1), Some(ss2)) => sign(ss1 - ss2)
    }
  }

  /* See section 20 at (Mehlhorn and Naher, 1994) */
  def compareSegments(s1: Segment, s2: Segment, p_sweep: Coordinate): Int = {
    cmp_segments(
      s1.source.x, s1.source.y,
      s2.source.x, s2.source.y,
      s2.target.x, s2.target.y,
      p_sweep.x, p_sweep.y,
      s1.dx, s1.dy,
      s2.dx, s2.dy
    )
  }

  /* See section 19 at (Mehlhorn and Naher, 1994) */
  private def cmp_segments(
     px: Double,  py: Double, // s1.source
    spx: Double, spy: Double, // s2.source
    sqx: Double, sqy: Double, // s2.target
     rx: Double,  ry: Double, // p_sweep (sweepline's current point)
     dx: Double,  dy: Double, // s1 delta x and y
    sdx: Double, sdy: Double  // s2 delta x and y
  ): Int = {

    /* Segments are identical */
    val sign1 = sign( dy * sdx - sdy *  dx)
    val areIdentical = if( sign1 == 0 || sign1 == NO_IDEA ){
      val mdx = sqx - px
      val mdy = sdy - py
      val sign2 = sign( dy * mdx - mdy *  dx)
      if( sign2 == 0 || sign2 == NO_IDEA ){
        val sign3 = sign(sdy * mdx - mdy * sdx)
        if( sign3 == 0 || sign3 == NO_IDEA ){
          if( sign1 == 0 && sign2 == 0 && sign3 == 0 ) {
            Some(0)
          } else {
            Some(NO_IDEA)
          }
        } else {
          None
        }
      } else {
        None
      }
    } else {
      None
    }
    
    areIdentical match {
      case Some(i) => i
      case None => { /* The underlaying segments are different */
        if( dx == 0 ) {  // if s1 is vertical...
          val a = spy * sdx - spx * sdy
          val b = sdy *  rx -  ry * sdx
          val i = sign( a + b )
          i match {
            case x if i <= 0 =>  1
            case x if i  > 0 => -1
            case _ => NO_IDEA
          }
        } else if( sdx == 0) { // if s2 is vertical...
          val a = py * dx - px * dy
          val b = dy * rx - ry * dx
          val i = sign( a + b )
          i match {
            case x if i <= 0 =>  1
            case x if i  > 0 => -1
            case _ => NO_IDEA
          }
        } else { // neither s1 nor s2 is vertical...
          val a = sdx * (  py *  dx +  dy * ( rx -  px) )
          val b =  dx * ( spy * sdx + sdy * ( rx - spx) )
          val sign2 = sign( a - b )
          if( sign2 == NO_IDEA ) NO_IDEA
          else if( sign2 != 0 ) sign2
          else {
            val c = py * dx - px * dy
            val d = dy * rx - ry * dx
            val sign3 = sign( c + d )
            sign3 match {
              case x if sign3 <= 0 =>  sign1
              case x if sign3  > 0 => -sign1
              case _ => NO_IDEA
            }
          }
        }
      }
    }


  }

  //////////////////////////////////////// Primitives [End] ////////////////////////////////////////

  def getIntersections(implicit geofactory: GeometryFactory): List[Intersection] = {
    findIntersections
    this.X.toList
  }

  def printStatus(filter: String = "*") = {
    filter match {
      case "*" => this.T.iterator().asScala.zipWithIndex
          .map{ case(s, x)  => s"$x\t$s" }.foreach{ println }
      case _   => this.T.iterator().asScala.filter(_.segments.head.label != filter).zipWithIndex
          .map{ case(s, x)  => s"$s\t$x" }.foreach{ println }
    }
    println
  }

  def findIntersections(implicit geofactory: GeometryFactory): Unit = {
    var j = 0
    val f = new java.io.PrintWriter("/tmp/edgesQQ.wkt")
    val g = new java.io.PrintWriter("/tmp/edgesTT.wkt")

    while(!this.Q.isEmpty /* && j < 100 */ ) {
      j = j + 1
      val e: Event  = this.Q.poll()
      val L: Double = e.value

      f.write(s"${e.wkt()}\t$j\n")

      e.ttype match {
        case 0 => {
          for{ s <- e.segments }{ 
            Tree.recalculate(L)
            val node = Node(s.value, ArrayBuffer(s))
            this.T.add(node)

            if( Tree.lower(s) != null ) {
              val r: Segment = Tree.lower(s)
              this.reportIntersection(r, s, L)
            }
            if( Tree.higher(s) != null ) {
              val t: Segment = Tree.higher(s)
              this.reportIntersection(t, s, L)
            }
            if( Tree.lower(s) != null && Tree.higher(s) != null ) {
              val r: Segment = Tree.lower(s)
              val t: Segment = Tree.higher(s)
              this.removeFuture(r, t);
            }

            T.iterator().asScala.foreach{ node =>
              val wkt = node.segments.map{ s =>
                s"POINT($L ${node.value})\t${s.label}${s.id}\t$j"
              }.mkString("\n")
              g.write(s"$wkt\n")
            }

          }
        }
        case 1 => {
          for{ s <- e.segments }{
            if(s.isVertical){

              println(j)
              println(s)
              printStatus(s.label)

            }
            if( Tree.lower(s) != null && Tree.higher(s) != null ) {
              val r: Segment = Tree.lower(s)
              val t: Segment = Tree.higher(s)
              this.reportIntersection(r, t, L)
            }

            T.iterator().asScala.foreach{ node =>
              val wkt = node.segments.map{ s =>
                s"POINT($L ${node.value})\t${s.label}${s.id}\t$j"
              }.mkString("\n")
              g.write(s"$wkt\n")
            }

            Tree.remove(s)
          }
        }
        case 2 => {
          val s_1: Segment = e.segments(0)
          val s_2: Segment = e.segments(1)
          Tree.swap(s_1, s_2)
          if( s_1.value < s_2.value ) {
            if(Tree.higher(s_1) != null) {
              val t: Segment = Tree.higher(s_1)
              this.reportIntersection(t, s_1, L)
              this.removeFuture(t, s_2)
            }
            if(Tree.lower(s_2) != null) {
              val r: Segment = Tree.lower(s_2)
              this.reportIntersection(r, s_2, L)
              this.removeFuture(r, s_1)
            }
          } else {
            if(Tree.higher(s_2) != null) {
              val t: Segment = Tree.higher(s_2)
              this.reportIntersection(t, s_2, L)
              this.removeFuture(t, s_1)
            }
            if(Tree.lower(s_1) != null) {
              val r: Segment = Tree.lower(s_1)
              this.reportIntersection(r, s_1, L)
              this.removeFuture(r, s_2)
            }
          }

          Tree.recalculate(L)
          T.iterator().asScala.foreach{ node =>
            val wkt = node.segments.map{ s =>
              s"POINT($L ${node.value})\t${s.label}${s.id}\t$j"
            }.mkString("\n")
            g.write(s"$wkt\n")
          }

          if( s_1.label != s_2.label ) {
            this.X.append(Intersection(e.point, s_1, s_2))
          }

        }
      }
    }
    f.close()
    g.close()
  }

  def reportIntersection(s_1: Segment, s_2: Segment, L: Double): Unit = {
    val x1 = s_1.first.x
    val y1 = s_1.first.y
    val x2 = s_1.second.x
    val y2 = s_1.second.y

    val x3 = s_2.first.x
    val y3 = s_2.first.y
    val x4 = s_2.second.x
    val y4 = s_2.second.y
    
    val r = (x2 - x1) * (y4 - y3) - (y2 - y1) * (x4 - x3)

    if( r != 0 ) {
      val t = ((x3 - x1) * (y4 - y3) - (y3 - y1) * (x4 - x3)) / r
      val u = ((x3 - x1) * (y2 - y1) - (y3 - y1) * (x2 - x1)) / r
      
      if( t >= 0 && t <= 1 && u >= 0 && u <= 1 ) { // Find intersection point...
        val x_c = x1 + t * (x2 - x1)
        val y_c = y1 + t * (y2 - y1)

        if( L < x_c ) { // Right to the sweep line...
	  val point = new Coordinate(x_c, y_c)
	  val segs  = List(s_1, s_2)
	  // Add to scheduler...
	  this.Q.add(Event(point, segs, 2))
        }
      }
    }
  }

  def removeFuture(s_1: Segment, s_2: Segment): Unit = {
    val event = this.Q.asScala.filter(_.ttype == 2).find{ e =>
      (e.segments(0) == s_1 && e.segments(1) == s_2) ||
      (e.segments(0) == s_2 && e.segments(1) == s_1)
    }
    event match {
      case Some(e) => this.Q.remove(e)
      case None    =>  
    }
  }
}

/************************************************************************************/
/*****   Evnet case class             ***********************************************/
/************************************************************************************/

case class Event(point: Coordinate, segments: List[Segment], ttype: Int)
  (implicit geofactory: GeometryFactory) extends Ordered[Event] {

  val segsIds = s"${segments.map(_.id).mkString(",")}"

  val value = point.x

  /* compare function asked at LEDA boot pag 739 */
  def compare(that: Event): Int = {
    val C = this.point.x compare that.point.x

    val R = C match{
      case 0 => this.point.y compare that.point.y
      case _ => C
    }

    R
  }

  def sweepAsWKT(minY: Double = 0.0, maxY: Double = 1000.0): String = {
    s"LINESTRING( $value $minY, $value $maxY )"
  }

  def asJTSLine: LineString = geofactory.createLineString(
    Array(new Coordinate(value, 0), new Coordinate(value, 1000))
  )

  def wkt(minY: Double = 0.0, maxY: Double = 1000.0): String = {
    val coords = s"${point.x} ${point.y}"
    val segs = segments.map{ seg => s"${seg.label}${seg.id}" }.mkString(" ")

    s"${sweepAsWKT(minY, maxY)}\t$value\t$coords\t$ttype\t$segs"
  }

  override def toString: String = {
    val coords = s"(${point.x} ${point.y})"
    val wkt = s"POINT${coords}"
    val segs = segments.map(_.id).mkString(", ")

    f"$wkt%-25s value: ${value} event_point: $coords type: $ttype segs_id: { $segs }"
  }
}

/************************************************************************************/
/***************************   Segment case class   *********************************/
/************************************************************************************/

case class Segment(h: Half_edge, label: String)(implicit geofactory: GeometryFactory){

  val p_1:    Coordinate = h.v1
  val p_2:    Coordinate = h.v2
  val source: Coordinate = h.v1
  val target: Coordinate = h.v2
  val id:     Long = h.id
  val line:   LineString = h.edge
  val lid:    String = s"${label}${id}"
  val angle:  Double = hangle(source, target)
  var value:  Double = this.calculateValue(this.first.x)
  var sweep:  Coordinate = new Coordinate(Double.MinValue, Double.MinValue)
  val debug:  Boolean = false

  def dx: Double = target.x - source.x
  def dy: Double = target.y - source.y
  def slope: Option[Double] = {
    if( dx == 0 ){
      None
    } else {
      Some( dy / dx )
    }
  }

  def envelope: Envelope = h.edge.getEnvelopeInternal

  def within(that: Segment): Boolean = this.h.edge.within(that.h.edge)

  def identical(that: Segment): Boolean = this.source == that.source && this.target == that.target

  def overlaps(that:Segment): Boolean = this.within(that) || that.within(this)

  def isTrivial(sweep: Coordinate): Boolean = this.source == sweep && this.target == sweep  

  def first: Coordinate = {
    if( p_1.x < p_2.x ) { p_1 }
    else if( p_1.x > p_2.x ) { p_2 }
    else {
      if( p_1.y < p_2.y ) { p_1 }
      else { p_2 }
    }
  }
  
  def second: Coordinate = {
    if( p_1.x < p_2.x ) { p_2 }
    else if( p_1.x > p_2.x ) { p_1 }
    else {
      if( p_1.y < p_2.y ) { p_2 }
      else { p_1 }
    }
  }

  def calculateValue(value: Double): Double = {
    val x1 = this.first.x; val x2 = this.second.x
    val y1 = this.first.y; val y2 = this.second.y

    val dx = x2 - x1 // TODO: Track Zero division...
    val dy = y2 - y1

    val vx = value - x1
    
    y1 + ( (dy / dx) * vx ) // TODO: NaN value does not seem to affect...
  }

  def calculateValue2(value: Double): Option[Double] = {
    val x1 = this.first.x; val x2 = this.second.x
    val y1 = this.first.y; val y2 = this.second.y

    val dx = x2 - x1 // TODO: Track Zero division...
    val V = if(dx == 0){
      None
    } else {
      val dy = y2 - y1
      val vx = value - x1
      
      Some( y1 + ( (dy / dx) * vx ) )
    }
    println(s"segment: ${this.wkt}\tvalue: $V")

    V
  }

  def isVertical: Boolean = dx == 0

  def isHorizontal: Boolean = dy == 0

  def isLeftOriented: Boolean = source.x > target.x

  def isUpwardsOriented: Boolean = source.y < target.y

  def isDegenerate: Boolean = {
    if(isVertical){
      true
    } else {
      false
    }
  }

  def reverse: Segment = {
    val edge = line.reverse().asInstanceOf[LineString]
    edge.setUserData(line.getUserData)
    val h = Half_edge(edge)
    h.id = this.h.id

    Segment(h, this.label)
  }

  def intersection(that: Segment): Option[Coordinate] = {
    val coords = this.line.intersection(that.line).getCoordinates
    if(coords.size == 1){
      Some(coords.head)
    } else {
      None
    }
  }

  def intersectionY(that_line: LineString): Double = {
    val this_line = this.h.edge
    if ( this_line.intersects(that_line) ) {
      this_line.intersection(that_line).getCentroid.getY
    } else {
      Double.MinValue
    }
  }
  
  def asJTSLine: LineString = {
    val line = this.h.edge
    line.setUserData(s"$label\t$id\t$value")
    line
  }

  def wkt: String = s"${asJTSLine.toText}\t${label}${id}\t$value"

  private def hangle(p_1: Coordinate, p_2: Coordinate): Double = {
    val dx = p_1.x - p_2.x
    val dy = p_1.y - p_2.y
    val length = math.sqrt( (dx * dx) + (dy * dy) )
    val angle = if(dy > 0){
      math.acos(dx / length)
    } else {
      2 * math.Pi - math.acos(dx / length)
    }
    math.toDegrees(angle)
  }

  override def toString: String =
    f"${asJTSLine.toText}%-30s label: ${label}%-4s id: ${id}%-3s value: ${this.value}%-20s"
}