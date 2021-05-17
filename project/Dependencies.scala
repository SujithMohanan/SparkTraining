package com.training

import sbt._

object Dependencies extends AutoPlugin {
  object AutoImport {

    object SparkDeps {
      val version = "3.1.1"
      val core  = "org.apache.spark" %% "spark-core" % version
      val sql   = "org.apache.spark" %% "spark-sql" % version
    }
  }

}