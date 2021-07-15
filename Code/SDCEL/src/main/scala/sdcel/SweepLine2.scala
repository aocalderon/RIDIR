package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geomgraph.index.SimpleMCSweepLineIntersector
import com.vividsolutions.jts.geomgraph.index.SegmentIntersector
import com.vividsolutions.jts.algorithm.RobustLineIntersector
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.geom.Coordinate
import com.vividsolutions.jts.geomgraph.EdgeIntersection
import com.vividsolutions.jts.geom.{LinearRing, LineString}
import com.vividsolutions.jts.geomgraph.Edge
import scala.collection.JavaConverters._
import scala.annotation.tailrec
import edu.ucr.dblab.sdcel.geometries.{Segment, Half_edge, EdgeData}
import scala.collection.mutable.Queue

object SweepLine2 {  
  /* For those half-edges which do not touch the cell */
  def getHedgesInsideCell(innerEdges: Vector[LineString], index: Int = -1)
    (implicit geofactory: GeometryFactory): Vector[List[Half_edge]] = {
    val innerHedges = innerEdges.map( inner => Half_edge(inner))

    innerHedges.groupBy{in => (in.data.polygonId, in.data.ringId)}
      .flatMap{ case(pid, hedges_prime) =>
        val hedges = hedges_prime.sortBy(_.data.edgeId).toList
        getLineSegments(hedges.tail, List(hedges.head), Vector.empty[List[Half_edge]])
      }.toVector
  }
  @tailrec
  def getLineSegments(hedges: List[Half_edge], segment: List[Half_edge],
    segments: Vector[List[Half_edge]]): Vector[List[Half_edge]] = {

    hedges match {
      case Nil => segments :+ segment
      case head +: tail => {
        val (new_current, new_segments) = {
          val prev = segment.last.data.edgeId + 1
          val next = head.data.edgeId
          if( prev == next ){
            (segment :+ head, segments)
          }
          else (List(head), segments :+ segment)
        }
        getLineSegments(tail, new_current, new_segments)
      }
    }
  }

  /* For those half-edges which touch the cell */
  object Mode  extends Enumeration {
    type Mode = Value
    val On, In, Out = Value
  }
  import Mode._

  case class ConnectedIntersection(id: Int, coord: Coordinate,
    hins: Queue[Half_edge] = Queue.empty[Half_edge],
    houts: Queue[Half_edge] = Queue.empty[Half_edge]){

    var next: ConnectedIntersection = null
  }

  case class Intersection(coord: Coordinate, hedge: Half_edge, mode: Mode)

  def getHedgesTouchingCell(outerEdges: Vector[LineString], mbr: LinearRing,
    index: Int = -1)
    (implicit geofactory: GeometryFactory): Vector[List[Half_edge]] = {

    // Converting my edges in Edge Java class...
    val edgesList = outerEdges.map{ linestring =>
      new com.vividsolutions.jts.geomgraph.Edge(linestring.getCoordinates)
    }.asJava

    // Converting cell edges in Edge Java class...
    val mbrList = mbr.getCoordinates.zip(mbr.getCoordinates.tail).toList.map{case(p1, p2) =>
      new com.vividsolutions.jts.geomgraph.Edge(Array(p1, p2))
    }.asJava

    // Setting Sweepline java class...
    val sweepline = new SimpleMCSweepLineIntersector()
    val lineIntersector = new RobustLineIntersector()
    lineIntersector.setMakePrecise(geofactory.getPrecisionModel)
    val segmentIntersector = new SegmentIntersector(lineIntersector, true, true)

    // Finding intersections...
    sweepline.computeIntersections(edgesList, mbrList, segmentIntersector)

    // Getting list of the intersections (coordinates) on cell border...
    val iOn = getEdgesOn(mbrList).map{ edge =>
      val hon = Half_edge(edge)
      Intersection(hon.v1, hon, Mode.On) // we collect the origen of the edges on border...
    }

    // Getting hedges coming to the border (in) and leaving the border (out)...
    val (edgesIn, edgesOut) = getEdgesTouch(edgesList, outerEdges, mbr, index)
    val iIn = edgesIn.map{ edge =>
      val hin = Half_edge(edge)
      Intersection(hin.v2, hin, Mode.In) // hin comes to the border at coord v2...
    }
    val iOut = edgesOut.map{ edge =>
      val hout = Half_edge(edge)
      Intersection(hout.v1, hout, Mode.Out) // hout leaves the border at coord v1... 
    }

    // Grouping hedges in and out by their coordinate intersections...
    val intersections = iOn union iIn union iOut
    val ring = intersections.groupBy(_.coord).map{ case(coord, inters) =>
      val id = inters.filter(_.mode == On).head.hedge.data.edgeId
      val hins = new Queue[Half_edge]
      hins ++= inters.filter(_.mode == In).map(_.hedge)
      val houts = new Queue[Half_edge]
      houts ++= inters.filter(_.mode == Out).map(_.hedge)

      ConnectedIntersection(id, coord, hins, houts)
    }.toList.sortBy(_.id)
    
    // Creating the circular linked list over the coordinate intersections...
    ring.zip(ring.tail).foreach{ case(c1, c2) => c1.next = c2}
    ring.last.next = ring.head

    // Finding the number of half-edges involved...
    val n = ring.filter(!_.hins.isEmpty).size

    // Extracting the list of half-edges...
    val r = getHedgesList(0, n, ring.head, Vector.empty[List[Half_edge]])

    r
  }

  @tailrec
  private def getHedgesList(i: Int, n: Int, head: ConnectedIntersection, 
    v: Vector[List[Half_edge]])
    (implicit geofactory: GeometryFactory): Vector[List[Half_edge]] = {
    if(i == n){
      v
    } else {
      val (start, hin) = nextIn(head)
      val hList = collectHedgesUntilNextOut(start, hin)
      getHedgesList(i + 1, n, head, v :+ hList)
    }
  }

  // Getting list of coordinates which intersect the cell border...
  private def getEdgesOn(mbrList: java.util.List[Edge])
      (implicit geofactory: GeometryFactory): Vector[LineString] = {
    val coords = mbrList.asScala.flatMap{ edge =>
      val start = edge.getCoordinates.head
      val inners = edge.getEdgeIntersectionList.iterator.asScala.map{ i =>
        i.asInstanceOf[EdgeIntersection].getCoordinate
      }.toVector

      start +: inners
    }.toVector :+ mbrList.asScala.last.getCoordinates.last // Adding last coordinate...
    coords.zip(coords.tail).zipWithIndex.map{ case(pair, id) =>
      val p1 = pair._1
      val p2 = pair._2
      val segment = geofactory.createLineString(Array(p1, p2))
      segment.setUserData(EdgeData(-1,0,id,false))
      segment
    }    
  }

  @tailrec
  private def nextIn(i: ConnectedIntersection): (ConnectedIntersection, Half_edge) = {
    if(!i.hins.isEmpty) (i, i.hins.dequeue) else nextIn(i.next)
  }

  private def collectHedgesUntilNextOut(i: ConnectedIntersection,
    start: Half_edge)
    (implicit geofactory: GeometryFactory): List[Half_edge] = {

    @tailrec
    def nextOut(i: ConnectedIntersection, pid: Int,
      iList: List[ConnectedIntersection]): (List[ConnectedIntersection], Half_edge) = {
      if(i.houts.map(_.data.polygonId).contains(pid)){
        val hout = i.houts.dequeueFirst(_.data.polygonId == pid).get
        (iList :+ i, hout)
      } else nextOut(i.next, pid, iList :+ i)
    }

    val pid = start.data.polygonId
    val rid = start.data.ringId
    val isHole = start.data.isHole
    val (iList, end) = nextOut(i, pid, List.empty[ConnectedIntersection])
    if(iList.isEmpty){
      List(start, end)
    } else {
      val hedges = iList.zip(iList.tail).map{ case(i1, i2) =>
        val c1 = i1.coord
        val c2 = i2.coord
        val edge = geofactory.createLineString(Array(c1, c2))
        edge.setUserData(EdgeData(pid,rid,i1.id, isHole))
        Half_edge(edge)
      }
      start +: hedges :+ end
    }
  }

  private def getEdgesTouch(edgesList: java.util.List[Edge],
    outerEdges: Vector[LineString], mbr: LinearRing, index: Int = -1)
    (implicit geofactory: GeometryFactory): (Vector[LineString],Vector[LineString])  = {

    case class T(coords: Array[Coordinate], data: EdgeData, isIn: Boolean)
    val outerEdgesMap = outerEdges.map{ edge => edge.getCoordinates -> edge }.toMap
    val envelope = mbr.getEnvelopeInternal
    val (edgesTouchIn, edgesTouchOut) = edgesList.asScala.map{ edge =>
      val extremes = edge.getCoordinates
      val data = outerEdgesMap(extremes).getUserData.asInstanceOf[EdgeData]
      val start = extremes(0)
      val end = extremes(1)

      val eiList = edge.getEdgeIntersectionList.iterator.asScala.toList

      if(eiList.length == 1){
        val intersection = eiList.head.asInstanceOf[EdgeIntersection].getCoordinate
        if(start != intersection && end != intersection &&
          envelope.contains(start) && !envelope.contains(end))
          List(T(Array(start, intersection), data, true))
        else if(start != intersection && end != intersection &&
          !envelope.contains(start) && envelope.contains(end))
          List(T(Array(intersection, end), data, false))
        else if(end == intersection && envelope.contains(start))
          List(T(Array(start, intersection), data, true))
        else if(start == intersection && envelope.contains(end))
          List(T(Array(intersection, end), data, false))
        else
          List(T(Array.empty[Coordinate], data, false))
      } else if(eiList.length == 2){
        val is = eiList.map(_.asInstanceOf[EdgeIntersection].getCoordinate)
        List(T(Array(is.head, is.last), data, true),
        T(Array(is.head, is.last), data, false))
      } else {
        List(T(Array.empty[Coordinate], data, false))
      }
    }.flatten.filterNot(_.coords.isEmpty).map{ t =>
        val segment = geofactory.createLineString(t.coords)
        segment.setUserData(t.data)
        (t.isIn, segment)
    }.partition(_._1)

    val in = edgesTouchIn.map(_._2).toVector
    val out = edgesTouchOut.map(_._2).toVector

    (in, out)
  }

  /* Functions for concatenate segments */
  @tailrec
  private def findNext(s1: Segment, ss: List[Segment]): (Segment, List[Segment]) = {
    val last = s1.last.v2
    val (s, new_ss) = ss.partition{_.first.v1 == last}

    if(s.isEmpty){
      (s1, new_ss)
    } else {
      val s2 = s.head
      val hedges = s1.hedges ++ s2.hedges
      val new_s1 = Segment(hedges)
      findNext(new_s1, new_ss)
    }
  }
  @tailrec
  private def concatSegments(segments: List[Segment], r: List[Segment]): List[Segment] = {
    if(segments.isEmpty){
      r
    } else {
      val (r1, s1) = findNext(segments.head, segments.tail)
      concatSegments(s1, r :+ r1)
    }
  }
  def merge(outer: Vector[List[Half_edge]],
    inner: Vector[List[Half_edge]], partitionId: Int = -1)
    (implicit geofactory: GeometryFactory): Iterable[Half_edge] = {

    val sin  = inner.map{hs => Segment(hs.distinct)}
    val sout = outer.map{hs => Segment(hs.distinct)}

    val segments0 = (sin ++ sout).groupBy(_.polygonId).values.toList
    if(partitionId == 29){
      segments0.filter(_.head.polygonId == 34).map(_.distinct).foreach{println}
    }

    val segments = segments0.map{ s =>
      // I have to purge the list of segments to fix the bug of hedge touching two cell borders...
      //val segs = findExtensions(s, Vector.empty[Segment]).toList
      val segs = s.toList

      concatSegments(segs, List.empty[Segment])
    }

    segments.flatMap{ ss =>
        ss.map{ s =>
          val h = s.hedges
          h.zip(h.tail).foreach{ case(current, next) =>
            current.next = next
            next.prev = current
          }
          h.last.next = h.head
          h.head.prev = h.last
          h.head
        }
      }
  }

  // Start: Functions to fix bug when half-edge touch two cell borders at the same time...
  def checkExtension[T](x: Segment, borders: Vector[Segment]): Boolean = {
    borders.exists { b  => b.last == x.first || b.first == x.last }
  }

  def extend[T](x: Segment, borders: Vector[Segment]): Segment = {
    val left  = borders.foldLeft(x){ (l, r) =>
      if(l.first == r.last)
        Segment(r.hedges ++ l.hedges.tail)
      else
        l
    }
    val right = borders.foldLeft(x){ (l, r) =>
      if(l.last == r.first)
        Segment(l.hedges ++ r.hedges.tail)
      else
        l
    }

    Segment(left.hedges.slice(0, left.hedges.length - x.hedges.length) ++ right.hedges)
  }

  @tailrec
  def findExtensions(borders: Vector[Segment], result: Vector[Segment])
      : Vector[Segment] = {

    val (extendibles, isolates) = borders.partition( b => checkExtension(b, borders))
    val new_result = result ++ isolates
    if(extendibles.isEmpty){
      new_result
    } else {
      val first = extendibles.head
      val tail  = extendibles.tail
      val extension = extend(first, tail)

      val new_borders = extendibles.filter{ x =>
        extension.hedges.intersect(x.hedges).isEmpty } ++ List(extension)

      findExtensions(new_borders, new_result)
    }
  }
  // End: Functions to fix bug when half-edge touch two cell borders at the same time...
}
