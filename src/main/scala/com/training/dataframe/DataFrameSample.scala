package com.training.dataframe

import org.apache.spark.sql.SparkSession

object DataFrameSample {

  def main(args: Array[String]): Unit = {

    ///creating spark session
    val sparkSession = SparkSession.builder()
      .appName("DataFrameTest")
      .master("local[2]")
      .getOrCreate()

    //inputfile
    val file = "/home/sujith/Downloads/baseball.csv"

    //reading file
    val df = sparkSession.read
      .option("header", true)
      .csv(file)

    //display contents
    df.show()

    //closing spark session
    sparkSession.close()
  }
}
