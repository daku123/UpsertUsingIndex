package com.datametica.spark.upsert

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.collection.mutable.ListBuffer

object Schema {

  def readSchemaAndCreateTable(spark:SparkSession,indexTableName:String,baseTableName:String,tableLocation:String,
                               schemaFilePath:String,partitionColumn:String,primaryKeys:String,dataFrame:DataFrame,
                               versionCol:String)={

    val schema = dataFrame.schema
    val fields = schema.fields
    var baseTableSchema = ""
    var indexTableSchema = ""
    var partCol = ""

    for (f <- fields) {
      val colName = f.name.replaceAll("\"","")
      val dataType = f.dataType.typeName

      if(!colName.equalsIgnoreCase(partitionColumn))
        baseTableSchema += colName + " " + dataType + ","
      else partCol+=colName + " " + dataType
      if(primaryKeys.contains(colName)) indexTableSchema+=colName + " " + dataType + ","
    }

    //creating Base table.
    TableQuery
      .createHiveTable(spark, baseTableName,tableLocation, "base",
        baseTableSchema.substring(0,baseTableSchema.length-1), partCol, versionCol
      )

    //creating index table
    TableQuery
      .createHiveTable(spark,indexTableName,tableLocation,"index",
        indexTableSchema.substring(0,indexTableSchema.length-1),"run_id int",versionCol
      )

  }


  def dataTypeMapper(pattern:String) = {
    pattern match {
      case "\"string\"" => StringType
      case "\"int\"" => IntegerType
    }
  }

  def createSchema(spark:SparkSession, schemaFilePath:String)={

    val listOfStructFields = new ListBuffer[StructField]
    val read = spark.read.textFile(schemaFilePath)

    var schema = ""
    read.collect().foreach{line => schema+=line}

    val arrayOfColAndType = schema.substring(1,schema.length-1).split(",")

    for (colAndType <- arrayOfColAndType){

      listOfStructFields += StructField(
        colAndType.split(":")(0).replaceAll("\"",""),
        this.dataTypeMapper(colAndType.split(":")(1)),true
      )
    }

    StructType(listOfStructFields.toList)
  }
}
