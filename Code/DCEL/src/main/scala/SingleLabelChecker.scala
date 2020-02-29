import org.apache.spark.rdd.RDD

object SingleLabelChecker{
  def parseId(id: String): String = id.split("\\|").distinct.sorted.mkString("|")

  def checkSingleLabels(dcels_prime: RDD[(LDCEL, LDCEL, LDCEL)]): RDD[(LDCEL, LDCEL, LDCEL)] = {
    dcels_prime.map{ dcels =>
      val facesM = dcels._1.faces
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
            A <- singleA if B.toPolygon().getCentroid.coveredBy(A.toPolygon())
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
            B <- singleB if A.toPolygon().getCentroid.coveredBy(B.toPolygon())
          } yield {
            B.id = parseId(A.id + "|" + B.id)
          }
        }
        case None => //logger.warn("No single labels with B")
      }
      dcels
    }
  }
}