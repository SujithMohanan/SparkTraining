import com.training.Dependencies.AutoImport._

name := "SparkTraining"

version := "0.1"

scalaVersion := "2.12.13"

libraryDependencies  ++= Seq {
  SparkDeps.core
  SparkDeps.sql
}

