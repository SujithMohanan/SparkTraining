package com.training.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, StructType}

object JSONRead {
  //creating SparkSession
  val sparkSession = SparkSession.builder()
    .appName("CSVRead")
    .master("local[2]")
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    /*
      run this only one time to get the json schema of input jsson file
      At that time, comment the method - afterGettingSchema
      After the run, copy the json schema to jsonSchema in afterGettingSchema variable
     */
    firstTimeLoad

    /*
      after initial run and getting the json schema, comment firstTimeLoad
      copy schema to variable jsonSchema. Then process data in json file
     */
    //afterGettingSchema

    //closing schema
    sparkSession.close()


  }

  /*
    Method to get Json schema
   */
  def  firstTimeLoad = {
    //create dataframe of input file
    val df = printSchema("/home/sujith/Downloads/color.json")
    //get jsonSchema
    val jsonSchema = df.schema.json
    println(jsonSchema)
  }

  /*
    Method to process data
   */
  def afterGettingSchema = {
    // Json schema from firstTimeLoad method
    val jsonSchema = "{\"type\":\"struct\",\"fields\":[{\"name\":\"colors\",\"type\":{\"type\":\"array\",\"elementType\":{\"type\":\"struct\",\"fields\":[{\"name\":\"color\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"value\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]},\"containsNull\":true},\"nullable\":true,\"metadata\":{}}]}"
    val schemaToProcess = DataType.fromJson(jsonSchema).asInstanceOf[StructType]
    readFileWithSchema("/home/sujith/Downloads/color.json", schemaToProcess).show()
  }

  def printSchema(file:String) = {
    sparkSession.read
      .option("multiLine", true) // to process multiline json
      .json(file)
  }

  def readFileWithSchema(file:String, schema:StructType) = {
    sparkSession.read
      .schema(schema)
      .option("multiLine", true) // to process multiline json
      .json(file)
  }
}
