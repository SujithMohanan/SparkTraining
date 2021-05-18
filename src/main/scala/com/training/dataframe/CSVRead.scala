package com.training.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object CSVRead {

  val sparkSession = SparkSession.builder()
    .appName("CSVRead")
    .master("local[2]")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    dynamicSchema
    staticSchema
  }

  def dynamicSchema = {
    noQuotes("/home/sujith/Downloads/baseball.csv").show()
    withQuotes("/home/sujith/Downloads/customer.csv").show()
    fewQuotes("/home/sujith/Downloads/customer_attendance.csv").show()
  }

  def noQuotes(file: String) = {
    sparkSession.read
      .option("header", true)
      .csv(file)
  }

  def withQuotes(file: String) = {
    sparkSession.read
      .option("quote", "\"")
      .option("ignoreLeadingWhiteSpace", true)
      .option("header", true)
      .csv(file)
  }

  def fewQuotes(file: String) = {
    sparkSession.read
      .option("quote", "\"")
      .option("ignoreLeadingWhiteSpace", true)
      .option("header", true)
      .csv(file)
  }

  def staticSchema = {
    withStruct("/home/sujith/Downloads/customer_attendance.csv").show()
    withDDL("/home/sujith/Downloads/customer_attendance.csv").show()
  }

  def withStruct(file: String) = {
    val schema = StructType(Array(StructField("Name", StringType, false),
      StructField("Dept", StringType, false),
      StructField("Attendance", IntegerType, false)))
    sparkSession.read
      .schema(schema)
      .option("quote", "\"")
      .option("ignoreLeadingWhiteSpace", true)
      .option("header", true)
      .csv(file)

  }

  def withDDL(file: String) = {
    val schema = "Name STRING, Dept STRING, Attendance INT"
    sparkSession.read
      .schema(schema)
      .option("quote", "\"")
      .option("ignoreLeadingWhiteSpace", true)
      .option("header", true)
      .csv(file)
  }
}
