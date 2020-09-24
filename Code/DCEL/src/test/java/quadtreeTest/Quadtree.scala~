import com.vividsolutions.jts.geom.Envelope
import scala.io.Source
import edu.ucr.dblab.{StandardQuadTree, QuadRectangle}

object Quadtree {  
  def create[T](boundary: Envelope, lineages: List[String]): StandardQuadTree[T] = {
    val maxLevel = lineages.map(_.size).max
    val quadtree = new StandardQuadTree[T](new QuadRectangle(boundary), 0, 1, maxLevel)
    quadtree.split()
    for(lineage <- lineages.sorted){
      val arr = lineage.map(_.toInt - 48)
      println(arr)
      var current = quadtree
      for(position <- arr.slice(0, arr.size - 1)){
        println(position)
        val regions = current.getRegions()
        println(regions.length)
        current = regions(position)
        if(current.getRegions == null){
          current.split()
        }
      }
    }
    quadtree.assignPartitionLineage()
    quadtree.assignPartitionIds()

    quadtree
  }
}
