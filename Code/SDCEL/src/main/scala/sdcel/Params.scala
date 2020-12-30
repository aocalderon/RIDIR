package edu.ucr.dblab.sdcel

import org.rogach.scallop._

class Params(args: Seq[String]) extends ScallopConf(args) {
  val scale:       ScallopOption[Double]  = opt[Double]  (default = Some(1000.0))
  val input1:      ScallopOption[String]  = opt[String]  (default = Some(""))
  val input2:      ScallopOption[String]  = opt[String]  (default = Some(""))
  val quadtree:    ScallopOption[String]  = opt[String]  (default = Some(""))
  val boundary:    ScallopOption[String]  = opt[String]  (default = Some(""))
  val partitions:  ScallopOption[Int]     = opt[Int]     (default = Some(16))
  val local:       ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val debug:       ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val save:        ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val output:      ScallopOption[String]  = opt[String]  (default = Some("/tmp"))

  val apath:  ScallopOption[String] = opt[String] (default = Some("edgesA.wkt"))
  val bpath:  ScallopOption[String] = opt[String] (default = Some("edgesB.wkt"))
  val qpath:  ScallopOption[String] = opt[String] (default = Some("quadtree.wkt"))
  val epath:  ScallopOption[String] = opt[String] (default = Some("boundary.wkt"))
  val filter: ScallopOption[String] = opt[String] (default = Some("*"))

  verify()
}

