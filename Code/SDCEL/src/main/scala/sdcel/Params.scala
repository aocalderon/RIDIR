package edu.ucr.dblab.sdcel

import org.rogach.scallop._

class Params(args: Seq[String]) extends ScallopConf(args) {
  val tolerance:   ScallopOption[Double]  = opt[Double]  (default = Some(0.001))
  val scale:       ScallopOption[Double]  = opt[Double]  (default = Some(1000))
  val input1:      ScallopOption[String]  = opt[String]  (default = Some(""))
  val input2:      ScallopOption[String]  = opt[String]  (default = Some(""))
  val quadtree:    ScallopOption[String]  = opt[String]  (default = Some(""))
  val boundary:    ScallopOption[String]  = opt[String]  (default = Some(""))
  val bycapacity:  ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val readid:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val partitions:  ScallopOption[Int]     = opt[Int]     (default = Some(16))
  val partition:   ScallopOption[Int]     = opt[Int]     (default = Some(0))
  val master:      ScallopOption[String]  = opt[String]  (default = Some("local[4]"))

  val fraction:    ScallopOption[Double]  = opt[Double]  (default = Some(0.01))
  val maxentries:  ScallopOption[Int]     = opt[Int]     (default = Some(100))
  val maxlevel:    ScallopOption[Int]     = opt[Int]     (default = Some(12))

  val local:       ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val debug:       ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val loadsdcel:   ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val savesdcel:   ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val overlay:     ScallopOption[Boolean] = opt[Boolean] (default = Some(true))
  val ooption:     ScallopOption[Int]     = opt[Int]     (default = Some(0))
  val olevel:      ScallopOption[Int]     = opt[Int]     (default = Some(4))
  val output:      ScallopOption[String]  = opt[String]  (default = Some("tmp/"))
  val persistance: ScallopOption[Int]     = opt[Int]     (default = Some(1))

  val apath:  ScallopOption[String] = opt[String] (default = Some("edgesA.wkt"))
  val bpath:  ScallopOption[String] = opt[String] (default = Some("edgesB.wkt"))
  val qpath:  ScallopOption[String] = opt[String] (default = Some("quadtree.wkt"))
  val epath:  ScallopOption[String] = opt[String] (default = Some("boundary.wkt"))
  val qtag:   ScallopOption[String] = opt[String] (default = Some("tag"))
  val filter: ScallopOption[String] = opt[String] (default = Some("*"))

  verify()
}

