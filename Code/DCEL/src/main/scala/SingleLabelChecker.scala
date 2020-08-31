import org.apache.spark.rdd.RDD
import DCELMerger.logger

object SingleLabelChecker{
  def parseId(id: String): String = id.split("\\|").distinct.sorted.mkString("|")

  def checkSingleLabels(dcels_prime: RDD[(LDCEL, LDCEL, LDCEL)]): RDD[(LDCEL, LDCEL, LDCEL)] = {
    dcels_prime.mapPartitionsWithIndex{ case(index, iter) =>
      val dcels = iter.next()
      val facesM = dcels._1.faces.filterNot(_.id.contains("|"))

      val singleA = facesM.filter(_.id.substring(0,1) == "A")
      val facesB = if(singleA.size > 0){
        Some(dcels._3.faces)
      } else {
        None
      }

      facesB match {
        case Some(faces) => {
          for{
            B <- faces
            A <- singleA if {
              try{
                A.toPolygon().getInteriorPoint.coveredBy(B.toPolygon())
              } catch{
                case e: com.vividsolutions.jts.geom.TopologyException => {
                  val wktA = A.toPolygon.toText()
                  val wktB = B.toPolygon.toText()
                  logger.info(s"Face A:\n $wktA \n")
                  logger.info(s"Face B:\n $wktB \n")
                  false
                }
              }
            }
          } yield {
            A.id = parseId(A.id + "|" + B.id)
          }
        }
        case None => //logger.warn("No single labels with A")
      }

      val singleB = facesM.filter(_.id.substring(0,1) == "B")
      val facesA = if(singleB.size > 0){
        Some(dcels._2.faces)
      } else {
        None
      }

      facesA match {
        case Some(faces) => {
          for{
            A <- faces
            B <- singleB if {
              try{
                B.toPolygon().getInteriorPoint.coveredBy(A.toPolygon())
              } catch{
                case e: com.vividsolutions.jts.geom.TopologyException => {
                  val wktA = A.toPolygon.toText()
                  val wktB = B.toPolygon.toText()
                  logger.info(s"Face A:\n $wktA \n")
                  logger.info(s"Face B:\n $wktB \n")
                  false
                }
              }
            }
          } yield {
            B.id = parseId(A.id + "|" + B.id)
          }
        }
        case None => //logger.warn("No single labels with B")
      }
      Iterator(dcels)
    }
  }
}
