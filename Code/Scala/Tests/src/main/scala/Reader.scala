package edu.ucr.dblab.tests

import org.apache.spark.sql.SparkSession

object Reader {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate

    spark.close()
  }
}
