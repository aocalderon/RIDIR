name := "Testers"
organization := "edu.ucr.dblab"
version := "0.1"
scalaVersion in ThisBuild := "2.12.8"

val SparkVersion = "3.2.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % SparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % SparkVersion

//libraryDependencies += "org.slf4j" % "slf4j-jdk14" % "1.7.25"
libraryDependencies += "org.rogach" % "scallop_2.12" % "4.1.0"

libraryDependencies += "org.apache.sedona" % "sedona-python-adapter-3.0_2.12" % "1.1.1-incubating"
libraryDependencies += "org.apache.sedona" % "sedona-core-3.0_2.12" % "1.0.1-incubating"
libraryDependencies += "org.apache.sedona" % "sedona-sql-3.0_2.12" % "1.0.1-incubating"
libraryDependencies += "org.datasyslab" % "geotools-wrapper" % "1.1.0-25.2"
