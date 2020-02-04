//**
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
//**
object Tester extends App {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")
//**
  case class Vertex(x: Double, y: Double){
    override def toString: String = { s"$x $y" }

    def toWKT: String = { s"POINT(${toString})"}
  }

  case class Hedge(v1: Vertex, v2: Vertex, id: String, isTwin: Boolean = false){
    var twin: Hedge = null

    def toWKT: String = { s"LINESTRING(${v1.toString}, ${v2.toString})\t$id" }

    def asTwin: Hedge = { this.copy(v2, v1, id.head.toString(), true) }
  }
//**
  val v1 = Vertex(0,1)
  val v2 = Vertex(1,2)
  val v3 = Vertex(1,0)
  val v4 = Vertex(2,1)

  val h1 = Hedge(v1, v3, "A0")
  val h2 = Hedge(v3, v2, "A0")
  val h3 = Hedge(v2, v1, "A0")
  val h4 = Hedge(v3, v4, "A1")
  val h5 = Hedge(v4, v2, "A1")
  val h6 = Hedge(v2, v3, "A1")

  val l = Vector(h1,h2,h3,h4,h5,h6)
  l.map(_.toWKT).foreach{println}
  //**
  def filter(hedges: Vector[Hedge]): Vector[Hedge] = {
    hedges.map(h => ((h.v1, h.v2), h)).groupBy(_._1).values.map(_.map(_._2))
      .flatMap{ hedges =>
        println(hedges.sortBy(_.id).map(x => s"${x.toWKT} Twin: ${x.twin.id}").mkString(" <-> "))
        if(hedges.size > 1){
          val (h, t) = if(hedges(0).isTwin) (hedges(1), hedges(0)) else (hedges(0), hedges(1))
          h.twin = t.twin
          t.twin.twin = h
          Vector(h)
        } else {
          hedges
        }
      }.toVector
  }

  def pair(hedges: Vector[Hedge]): Vector[Hedge] = {
    hedges.map{ h =>
      if (h.isTwin) ((h.v2, h.v1), h) else ((h.v1, h.v2), h)
    }.groupBy(_._1).values.map(_.map(_._2))
      .flatMap{ h =>
        println(h.map(_.toWKT).mkString(" >< "))
        h(0).twin = h(1)
        h(1).twin = h(0)
        h
      }.toVector
  }

  val t = l.map(_.asTwin)
  val u = l.union(t)

  println("Union")
  u.foreach{println}

  println("Pair")
  val p = pair(u)
  p.foreach{println}

  println("Filter")
  val f = filter(p)
  f.foreach{println}

  val a = new java.io.PrintWriter("/tmp/edgesSprime.wkt")
  val c = f.map(x => s"${x.toWKT}\t${x.twin.id}\n")
  a.write(c.mkString(""))
  a.close()

  //**
}
