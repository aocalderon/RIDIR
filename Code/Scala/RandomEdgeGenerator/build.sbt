name := "REG"
organization := "UCR-DBLab"
version := "0.1"
scalaVersion in ThisBuild := "2.11.8"

val SparkVersion = "2.4.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % SparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % SparkVersion
libraryDependencies += "org.datasyslab" % "geospark" % "1.2.0"
libraryDependencies += "org.datasyslab" % "JTSplus" % "0.1.4"
libraryDependencies += "org.locationtech.jts" % "jts-core" % "1.16.1"
libraryDependencies += "org.slf4j" % "slf4j-jdk14" % "1.7.25"
libraryDependencies += "org.rogach" % "scallop_2.11" % "2.1.3"

libraryDependencies += "edu.ucr.dblab" % "utils_2.11" % "0.1"

libraryDependencies += "com.lihaoyi" % "ammonite" % "1.6.7" cross CrossVersion.full

mainClass in (Compile, run) := Some("REG")
mainClass in (Compile, packageBin) := Some("REG")
