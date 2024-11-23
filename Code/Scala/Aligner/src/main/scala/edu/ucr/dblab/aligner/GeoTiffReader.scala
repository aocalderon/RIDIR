package edu.ucr.dblab.aligner

import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import org.locationtech.jts.geom.GeometryFactory

object GeoTiffReader {
  def getInfo(geoTiffPath: String)(implicit G: GeometryFactory) = {
    // Read the GeoTIFF file
    val geoTiff: SinglebandGeoTiff = SinglebandGeoTiff(geoTiffPath)

    // Get raster data
    val raster: Raster[Tile] = geoTiff.raster

    // Print some information
    println(s"CRS: ${geoTiff.crs}")
    println(s"Extent: ${geoTiff.extent}")
    println(s"Cell Type: ${geoTiff.cellType}")
    println(s"Dimensions: ${geoTiff.cols}x${geoTiff.rows}")

    // Access individual pixel data (e.g., at the center of the raster)
    val tile = geoTiff.tile
    val (col, row) = (geoTiff.cols / 2, geoTiff.rows / 2)
    val pixelValue = tile.getDouble(col, row)
    println(s"Pixel value at ($col, $row): $pixelValue")

    val envelope = raster.geom.getEnvelopeInternal
    println(G.toGeometry(envelope).toText)
  }
}

