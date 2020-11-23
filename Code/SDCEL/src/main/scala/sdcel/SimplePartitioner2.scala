package edu.ucr.dblab.sdcel

import org.apache.spark.Partitioner

class SimplePartitioner[V](partitions: Int) extends Partitioner {
  def getPartition(key: Any): Int = {
    key.asInstanceOf[Int]
  }

  def numPartitions(): Int = {
    return partitions
  }
}
