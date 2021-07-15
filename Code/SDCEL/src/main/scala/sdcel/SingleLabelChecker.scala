package edu.ucr.dblab.sdcel

import com.vividsolutions.jts.geom.{LineString, Point}
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}

import org.apache.spark.rdd.RDD
import org.apache.spark.TaskContext

import edu.ucr.dblab.sdcel.geometries._

import DCELMerger2.groupByNext

object SingleLabelChecker{
  case class F(hedge: Half_edge, label: String)
  def checkSingleLabel(ldcel: RDD[(Half_edge, String)],
    sdcel: RDD[(Half_edge, String)], letter: String)
  (implicit geofactory: GeometryFactory)
      : RDD[(Half_edge, String)] = {
    ldcel.zipPartitions(sdcel, preservesPartitioning=true){ (itL, itS) =>
      val pid = TaskContext.getPartitionId
      val sdcel = itS.toList
      val hasSingles = sdcel.exists{ case(hedge, label) =>
        label.split(" ").size == 1
      }

      val partitionId = 39
      val update = if(hasSingles){
        
        val faces_ldcel = itL.toList
        if(pid == partitionId){
          //faces_ldcel.map{ case(h,l) => s"${h.getPolygon.toText}\t$l"}.foreach{println}
        }

        val (faces_sdcel1, faces_sdcel2) = sdcel.partition{ case(hedge, label) =>
          val isSingle = label.split(" ").size == 1
          val startWithLetter = label.substring(0,1) == letter

          isSingle & startWithLetter
        }
        if(pid == partitionId){
          //faces_sdcel1.map{ case(h,l) => s"${h.getPolygon.toText}\t$l"}.foreach{println}
        }

        val sdcel_labels_to_update = for{
          a <- faces_ldcel
          b <- faces_sdcel1 if{
            b._1.getPolygon.getInteriorPoint.coveredBy(a._1.getPolygon)
          }
        } yield {
          //val label1 = a._2
          //val label2 = b._2
          //val label = List(label1, label2).sorted.mkString(" ")
          //(b._1, label)
          (b, a._2)
        }

        val sdcel_labels_to_keep = faces_sdcel1.diff(sdcel_labels_to_update.map(_._1))

        val sdcel_updated = sdcel_labels_to_update.map{ l =>
          val hedge  = l._1._1
          val label1 = l._1._2
          val label2 = l._2
          val label = List(label1, label2).sorted.mkString(" ")
          (hedge, label)
        }

        if(pid == partitionId){
          //println("Updated")
          //sdcel_updated.map{ case(h,l) => s"${h.getPolygon.toText}\t$l"}.foreach{println}
          //println("Keeped")
          //faces_sdcel2.map{ case(h,l) => s"${h.getPolygon.toText}\t$l"}.foreach{println}
        }

        sdcel_updated ++ sdcel_labels_to_keep ++ faces_sdcel2
      } else {
        sdcel
      }
      update.toIterator
    }
  }
}
