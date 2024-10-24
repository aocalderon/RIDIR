name := "Tester"
organization := "edu.ucr.dblab"
version := "0.1"
scalaVersion in ThisBuild := "2.11.8"

val SparkVersion = "2.3.0"
val SparkCompatibleVersion = "2.3"
val GeoSparkVersion = "1.2.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % SparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % SparkVersion
libraryDependencies += "org.datasyslab" % "geospark" % GeoSparkVersion

libraryDependencies += "org.datasyslab" % "geospark-sql_".concat(SparkCompatibleVersion) % GeoSparkVersion
libraryDependencies += "org.datasyslab" % "geospark-viz_".concat(SparkCompatibleVersion) % GeoSparkVersion

libraryDependencies += "org.datasyslab" % "JTSplus" % "0.1.4"
libraryDependencies += "org.slf4j" % "slf4j-jdk14" % "1.7.25"
libraryDependencies += "org.rogach" % "scallop_2.11" % "2.1.3"

//libraryDependencies += "edu.ucr.dblab" % "utils_2.11" % "0.1"

mainClass in (Compile, run) := Some("edu.ucr.dblab.Tester")
mainClass in (Compile, packageBin) := Some("edu.ucr.dblab.Tester")
