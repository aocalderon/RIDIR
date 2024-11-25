package demo

import geotrellis.proj4.CRS
import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.resample.Bilinear
import geotrellis.vector.Extent

import scala.collection.parallel.ParSeq

object Main {
  case class SinglebandGeoTiffWithName(path: String){
    val band: SinglebandGeoTiff = SinglebandGeoTiff(path)
    val name: String = path.split("/").last.split("\\.").head
  }

  def main(args: Array[String]): Unit = {
    val geotiffs = Seq(
      "/home/and/tmp/arroz.tif",
      "/home/and/tmp/cacao.tif",
      "/home/and/tmp/cana.tif",
      "/home/and/tmp/maiz.tif",
      "/home/and/tmp/rellenos.tif",
      "/home/and/tmp/gas.tif"
    ).map( path => SinglebandGeoTiffWithName(path) ).par

    val extents: ParSeq[Extent] = geotiffs.map(_.band.extent)
    val cellSizes: ParSeq[CellSize] = geotiffs.map(_.band.raster.cellSize)

    val targetExtent: Extent = extents.reduce(_.combine(_))

    val targetCellSize: CellSize = cellSizes.minBy(_.resolution)

    val targetGrid: RasterExtent = RasterExtent(targetExtent, targetCellSize)

    val resampledRasters: ParSeq[(SinglebandRaster, String)] = geotiffs.map { geotiff =>
      val r: SinglebandRaster = geotiff.band.raster.resample(targetGrid, method = Bilinear)
      (r, geotiff.name)
    }

    val crs: CRS = geotiffs.head.band.crs
    resampledRasters.foreach{ case(raster, id) =>
      val singleband: SinglebandGeoTiff = SinglebandGeoTiff(raster.tile, raster.extent, crs)
      val output: String = s"/tmp/${id}_resampled.tif"
      singleband.write(output)
      println(s"Resampled GeoTIFFs saved in $output")
    }
  }
}
