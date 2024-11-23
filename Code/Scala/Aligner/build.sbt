ThisBuild / version      := "0.1.0"
ThisBuild / organization := "edu.ucr.dblab"
ThisBuild / scalaVersion := "2.11.12"

val SparkVersion = "2.4.0"

lazy val buildSettings = (project in file("."))
  .settings(
    name := "aligner",

    libraryDependencies += "org.apache.spark" %% "spark-core" % SparkVersion,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % SparkVersion,

    libraryDependencies += "org.locationtech.jts" % "jts-core" % "1.19.0",
    libraryDependencies += "org.locationtech.proj4j" % "proj4j" % "1.2.3",
    libraryDependencies += "org.locationtech.proj4j" % "proj4j-epsg" % "1.2.3",

    libraryDependencies += "org.rogach" % "scallop_2.11" % "2.1.3",

    libraryDependencies += "org.locationtech.geotrellis" %% "geotrellis-raster" % "3.5.1",
    libraryDependencies += "org.locationtech.geotrellis" %% "geotrellis-vector" % "3.5.1",
    
    libraryDependencies += "org.slf4j" % "slf4j-api" % "2.0.9",

    resolvers += "LocationTech Releases" at "https://repo.locationtech.org/content/groups/releases/"
  )
