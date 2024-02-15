ThisBuild / version      := "0.1.0"
ThisBuild / organization := "edu.ucr.dblab"
ThisBuild / scalaVersion := "2.11.12"
crossScalaVersions := Seq("2.11.12", "2.12.17")

val SparkVersion = "2.4.0"

lazy val buildSettings = (project in file("."))
	.settings(
	name := "sdcel",
	libraryDependencies += "org.apache.spark" %% "spark-core" % SparkVersion,
	libraryDependencies += "org.apache.spark" %% "spark-sql" % SparkVersion,
	libraryDependencies += "org.apache.spark" %% "spark-mllib" % SparkVersion,
	libraryDependencies += "org.datasyslab" % "geospark" % "1.2.0",
	libraryDependencies += "org.datasyslab" % "geospark-sql_2.3" % "1.2.0",
	libraryDependencies += "org.datasyslab" % "geospark-viz_2.3" % "1.2.0",
	libraryDependencies += "org.datasyslab" % "JTSplus" % "0.1.4",

	libraryDependencies += "org.jgrapht" % "jgrapht-core" % "1.4.0",

	libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.25",
	libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.25",

	libraryDependencies += "org.rogach" % "scallop_2.11" % "2.1.3",
	libraryDependencies += "ch.cern.sparkmeasure" %% "spark-measure" % "0.16",

	libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.12",
	libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.12" % "test",

	libraryDependencies += "com.google.guava" % "guava" % "31.1-jre"
)



