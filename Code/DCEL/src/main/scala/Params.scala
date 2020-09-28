package edu.ucr.dblab.sdcel

import org.rogach.scallop._

class Params(args: Seq[String]) extends ScallopConf(args) {
  val input1:      ScallopOption[String]  = opt[String]  (required = true)
  val offset1:     ScallopOption[Int]     = opt[Int]     (default = Some(0))
  val partitions:  ScallopOption[Int]     = opt[Int]     (default = Some(16))
  val local:       ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val debug:       ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val output:      ScallopOption[String]  = opt[String]  (default = Some("/tmp"))

  verify()
}

