package com.training.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode}

object ExplodeJson01 {
  //creating SparkSession
  val sparkSession = SparkSession.builder()
    .appName("ExplodeJson01")
    .master("local[2]")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val colorFile = getClass.getResource("/color.json").getFile
    val df = sparkSession.read
      .format("json")
      .option("multiLine", true)
      .load(colorFile)

    //Explode and extract data
    val df2 = df.withColumn("colors", explode(col("colors")))
      .withColumn("Name", col("colors.color"))
      .withColumn("HexValue", col("colors.value"))
      .drop("colors")

    df2.createOrReplaceTempView("color_json")
    val data = sparkSession.sql("select * from color_json")

    data.show()
    //closing schema
    sparkSession.close()
  }
}
