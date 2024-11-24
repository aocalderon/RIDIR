package edu.ucr.dblab.aligner

import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import org.locationtech.jts.geom.{GeometryFactory, Envelope}

import edu.ucr.dblab.aligner.Aligner.RasterName

object GeoTiffReader {

  def getRaster(raster_path: String): RasterName = {
    val geotiff: SinglebandGeoTiff = SinglebandGeoTiff(raster_path)
    val raster: Raster[Tile] = geotiff.raster
    val name: String = raster_path.split("/").last.split("\\.").head

    RasterName(name, raster)
  }

  def getInfo(R: RasterName): String = {
    val buffer = new StringBuilder(s"\n${R.name}:\n")
    buffer.append(s"Extent: ${R.raster.extent}\n")
    buffer.append(s"Cell Type: ${R.raster.cellType}\n")
    val cell: CellSize = R.raster.cellSize
    buffer.append(s"Cell Size: ${cell.width}x${cell.height} (${cell.resolution})\n")
    buffer.append(s"Dimensions: ${R.raster.cols}x${R.raster.rows}\n")

    buffer.toString
  }
}

