ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.8"

lazy val root = (project in file("."))
  .settings(
    name := "k-d_tree",
    idePackagePrefix := Some("edu.ucr.dblab.kdtree")
  )
