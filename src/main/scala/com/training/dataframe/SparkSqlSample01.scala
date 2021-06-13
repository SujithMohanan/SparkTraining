package com.training.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object SparkSqlSample01 {

  val sparkSession = SparkSession.builder()
    .appName("SparkSqlSample01")
    .master("local[2]")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val baseballFile = getClass.getResource("/baseball.csv").getFile

    val baseballDf = sparkSession.read
      .format("csv")
      .option("header",true)
      .option("ignoreLeadingWhiteSpace", true)
      .load(baseballFile)

    baseballDf
      .select(col("Name"), col("Team"))
      .filter(col("age") > 30)
      .show()

    baseballDf
      .select(col("Team"))
      .filter(col("age") > 30)
      .groupBy(col("Team")).count()
      .show()
  }

}
