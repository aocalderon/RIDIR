name := "PrecisionChecker"
organization := "edu.ucr.dblab"
version := "0.1"
scalaVersion in ThisBuild := "2.11.8"

val SparkVersion = "2.3.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % SparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % SparkVersion
libraryDependencies += "org.locationtech.jts" % "jts-core" % "1.18.0"
libraryDependencies += "org.datasyslab" % "geospark" % "1.2.0"
