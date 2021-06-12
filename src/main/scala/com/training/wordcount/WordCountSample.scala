package com.training.wordcount

import org.apache.spark.sql.SparkSession

object WordCountSample {

  def main(args: Array[String]): Unit = {


    val sparkSession = SparkSession.builder()
      .appName("WordCount")
      .master("local")
      .getOrCreate()

    val file  = getClass.getResource("/a.txt").getFile

    import sparkSession.implicits._
    val data = sparkSession.read.textFile(file).as[String]

    val counts = data.flatMap(value => value.split("\\s+"))
      .groupByKey(_.toLowerCase)
      .count()

    counts.show()
    sparkSession.close()


  }

}
