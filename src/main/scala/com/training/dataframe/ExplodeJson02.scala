package com.training.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode}

object ExplodeJson02 {

  //creating SparkSession
  val sparkSession = SparkSession.builder()
    .appName("ExplodeJson02")
    .master("local[2]")
    .getOrCreate()


  def main(args: Array[String]): Unit = {
    val colorFile = getClass.getResource("/colorNested.json").getFile
    val df = sparkSession.read
      .format("json")
      .option("multiLine", true)
      .load(colorFile)

    df.printSchema()

    val df2 = df.withColumn("colors", explode(col("colors")))
      .withColumn("category", col("colors.category"))
      .withColumn("color", col("colors.color"))
      .withColumn("type", col("colors.type"))
      .withColumn("hexcode", col("colors.code.hex"))
      .withColumn("rgbaCode", col("colors.code.rgba"))
      .drop(col("colors"))

    df2.show()

    sparkSession.close()
  }
}
