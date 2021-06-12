package com.training.dataframe

import com.training.dataframe.CSVRead.getClass
import org.apache.spark.sql.SparkSession

object DataFrameSample {

  def main(args: Array[String]): Unit = {

    ///creating spark session
    val sparkSession = SparkSession.builder()
      .appName("DataFrameTest")
      .master("local[2]")
      .getOrCreate()

    //inputfile
    val baseBallFile = getClass.getResource("/baseball.csv").getFile
    //reading file
    val df = sparkSession.read
      .option("header", true)
      .csv(baseBallFile)

    //display contents
    df.show()

    //closing spark session
    sparkSession.close()
  }
}
