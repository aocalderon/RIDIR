import org.apache.sedona.sql.utils.{Adapter, SedonaSQLRegistrator}
import org.apache.spark.serializer.KryoSerializer
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom.{PrecisionModel, GeometryFactory}
import org.locationtech.jts.geom.{Geometry, Coordinate}
import org.locationtech.jts.geom.Point
import org.apache.spark.sql.{Encoder, Encoders}

object TestEncoders = {
	def main(args: Array[String]) = {
		var spark = SparkSession.builder().master("local[*]").appName("SedonaTest").config("spark.serializer", classOf[KryoSerializer].getName).config("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName).getOrCreate()
		SedonaSQLRegistrator.registerAll(spark)
		import spark.implicits._

		val model = new PrecisionModel(1000)
		val geofactory = new GeometryFactory(model)

		implicit val PointEncoder: Encoder[Point] = org.apache.spark.sql.Encoders.kryo[Point]
		implicit val GeometryEncoder: Encoder[Geometry] = org.apache.spark.sql.Encoders.kryo[Geometry]

		val spDS = spark.createDataset( (0 to 1000).map{ x => geofactory.createPoint(new Coordinate(x,x))} )
		spDS.map{_.buffer(5.0)}.show
		spDS.map(_.buffer(5.0)).map(_.toText).show

		spark.close
	}
}
