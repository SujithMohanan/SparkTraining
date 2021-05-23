package com.training.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object CSVRead {

  //creating SparkSession
  val sparkSession = SparkSession.builder()
    .appName("CSVRead")
    .master("local[2]")
    .getOrCreate()

  ///Main method
  def main(args: Array[String]): Unit = {
    /**
     * Calling 2 methods
     *    dynamicSchema := Spark reads schema by its own
     *                    (But a task will get created to infer schema)
     *    staticSchema  := We will provide schema.
     *                    (So that spark can directly go to process data.)
     */
    dynamicSchema
    staticSchema

    //close sparksession
    sparkSession.close()
  }

  //method := dynamicSchema
  def dynamicSchema = {
    noQuotes("/home/sujith/Downloads/baseball.csv").show()
    withQuotes("/home/sujith/Downloads/customer.csv").show()
    fewQuotes("/home/sujith/Downloads/customer_attendance.csv").show()
  }

  //method to process csv without quotes
  def noQuotes(file: String) = {
    sparkSession.read
      .option("header", true)
      .csv(file)
  }

  //method to process csv with quotes
  def withQuotes(file: String) = {
    sparkSession.read
      .option("quote", "\"")  // to specify quote
      .option("ignoreLeadingWhiteSpace", true) //will ignore leading white spaces for cols
      .option("header", true) //header flag
      .csv(file)
  }

  //method to process csv having both quoted and normal cols
  def fewQuotes(file: String) = {
    sparkSession.read
      .option("quote", "\"")
      .option("ignoreLeadingWhiteSpace", true)
      .option("header", true)
      .csv(file)
  }

  //method := staticSchema
  def staticSchema = {
    withStruct("/home/sujith/Downloads/customer_attendance.csv").show()
    withDDL("/home/sujith/Downloads/customer_attendance.csv").show()
  }

  //method proving schema with StrutType
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

  //method proving schema with DDL
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
