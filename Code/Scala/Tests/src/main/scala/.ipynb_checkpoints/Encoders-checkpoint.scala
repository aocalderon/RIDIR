package edu.ucr.dblab.tests

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{Encoder, Encoders}

import org.locationtech.jts.geom.{Geometry, Envelope, Coordinate}
import org.locationtech.jts.geom.{Polygon, LineString, Point}

object GeoEncoders {
    implicit val PointEncoder:      Encoder[Point]      = Encoders.kryo[Point]
    implicit val LineStringEncoder: Encoder[LineString] = Encoders.kryo[LineString]
    implicit val PolygonEncoder:    Encoder[Polygon]    = Encoders.kryo[Polygon]
    implicit val GeometryEncoder:   Encoder[Geometry]   = Encoders.kryo[Geometry]
    implicit val EnvelopeEncoder:   Encoder[Envelope]   = Encoders.kryo[Envelope]
    implicit val CoordinateEncoder: Encoder[Coordinate] = Encoders.kryo[Coordinate]
}
