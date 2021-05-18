package com.training.dataframe

import org.apache.spark.sql.SparkSession

object DataFrameSample {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("DataFrameTest")
      .master("local[2]")
      .getOrCreate()

    val file = "/home/sujith/Downloads/baseball.csv"

    val df = sparkSession.read
      .option("header", true)
      .csv(file)
    df.show()
  }
}
