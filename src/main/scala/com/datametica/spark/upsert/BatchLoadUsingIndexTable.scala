package com.datametica.spark.upsert

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.json4s.jackson.Json

import scala.io.Source
import scala.util.parsing.json.JSON

object BatchLoadUsingIndexTable{

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = getSparkSession()
    spark.sparkContext.setLogLevel("ERROR")

    spark.sqlContext.setConf("hive.exec.dynamic.partition","true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.sqlContext.setConf("hive.metastore.try.direct.sql","false")
    spark.sqlContext.setConf("spark.sql.hive.manageFilesourcePartitions","false")

    val tableName="db_gold.myTable"
    val bucketLocation = "gs://self-staging-bucket/inputdata/*"
    val tableLocation = "gs://self-staging-bucket/outputdata"
    val format = "csv"
    historyLoad(spark,bucketLocation,tableLocation,tableLocation,tableName)
  }
  def getSparkSession(): SparkSession = {
    SparkSession.builder().appName("UsertUsingIndexTable")
      .enableHiveSupport().getOrCreate()

  }

  def historyLoad(spark:SparkSession,bucketLocation:String,dataFormat:String,tableLocation:String,tableName:String)={

    import org.apache.spark.sql.functions._

    val readDataFromBucket = spark.read.format(dataFormat).option("inferschema","true").option("header","true").csv(bucketLocation)
    //createHiveTable(spark,tableName,tableLocation,dataFormat,readDataFromBucket)
   // createIndexTable(spark,"db_gold.index",tableLocation)
    readSchemaAndCreateTable(spark,"db_gold.index",tableName,tableLocation,"schema.json","tsmt","id")
    val dataFrameWithVersion = readDataFromBucket.withColumn("version",lit(getVersionNumber(spark,"db_gold.index")))
    dataFrameWithVersion.write.mode(SaveMode.Append).insertInto(tableName)

    val changeDataForIndexTable = dataFrameWithVersion.selectExpr("id","version").withColumn("run_id",lit(getRunId(spark,"db_gold.index","next").toString))
    val  latestDataFromIndexTable = getLatestPartitionDatafromIndex(spark,"db_gold.index")
    val runId = getRunId(spark,"db_gold.index","next").toString
    val newIndexData = generateNewDataForIndexTable(spark,changeDataForIndexTable,latestDataFromIndexTable).withColumn("run_id",lit(runId))
    changeDataForIndexTable.show()
    latestDataFromIndexTable.show()
    newIndexData.show()
    newIndexData.write.mode(SaveMode.Append).insertInto("db_gold.index")
    //dataFrameWithVersion.selectExpr("id","version").write.mode(SaveMode.Append).insertInto("db_gold.index")
  }

  def createHiveTable(spark:SparkSession,tableName:String,tableLocation:String,
                      tableType:String,schema:String,partCol:String)={

    val baseTableLocation = tableLocation.concat("/baseTable")
    val indexTableLocation = tableLocation.concat("/indexTable")

    if (tableType.equalsIgnoreCase("base"))
      spark.sql(s"CREATE EXTERNAL TABLE IF NOT EXISTS ${tableName} (${schema}) PARTITIONED BY (${partCol},version int) " +
        s"STORED AS PARQUET LOCATION '${baseTableLocation}'")

    if (tableType.equalsIgnoreCase("index"))
    spark.sql(s"CREATE EXTERNAL TABLE IF NOT EXISTS ${tableName} ( ${schema},version int) " +
      s"PARTITIONED BY (run_id string) LOCATION '${indexTableLocation}' ")

  }

  def createSchemaForHiveTable(dataFrame:DataFrame):String={

    val my_schema = dataFrame.schema
    val fields = my_schema.fields
    var fieldStr = ""
    for (f <- fields) {
      fieldStr += f.name + " " + f.dataType.typeName + ","
    }
    return fieldStr.substring(0,fieldStr.length-1)
  }
  // write a function to read json file to create schema.

  //write logic for creating version
def getVersionNumber(spark:SparkSession,indexTableName:String):Int={
  val vNumber = spark.sql(s"select coalesce(max(version),0) as v from ${indexTableName}")
  val version = vNumber.select("v").collect().map(_.getInt(0))
  version(0)+1
}

  /**
    * get run-id from index table.
    * @param spark
    * @param indexTableName
    * @return
    */
  def getRunId(spark:SparkSession,indexTableName:String,typeOfRunId:String):Int={
    val runIdDataFrame = spark.sql(s"select coalesce(max(run_id),0) as run_id from ${indexTableName}")
    val maxRunId = runIdDataFrame.select("run_id").collect().map(_.getString(0))
    if (typeOfRunId.equalsIgnoreCase("next"))
    maxRunId(0).toInt+1
    else maxRunId(0).toInt
  }

  //create index table.
def createIndexTable(spark:SparkSession,indexTableName:String,tableLocation:String,schema:String)={
  val indexTableLocation = tableLocation.concat("/indexTable")
  spark.sql(s"CREATE EXTERNAL TABLE IF NOT EXISTS ${indexTableName} ( ${schema},version int) PARTITIONED BY (run_id string) LOCATION '${indexTableLocation}' ")
}

  /**
    * get all the data of latest partitions from index table.
    */
  def getLatestPartitionDatafromIndex(spark:SparkSession,indexTableName:String)={
    val maxRunId = getRunId(spark,indexTableName,"max").toString
    spark.table(indexTableName).where(s"run_id==${maxRunId}")
  //  spark.sql(s"select * from ${indexTableName} where run_id=${maxRunId}")

  }

  def generateNewDataForIndexTable(spark:SparkSession,newSetOfData:DataFrame,latestDataFromIndexTable:DataFrame)={
    val unionOfDataFrame = newSetOfData.union(latestDataFromIndexTable)
   import spark.implicits._
    val windowSpec = Window.partitionBy("id").orderBy('run_id desc)
    unionOfDataFrame.withColumn("row_number",row_number().over(windowSpec)).filter("row_number==1").drop("row_number")
   // spark.sql("select * from (select id,version,run_id,row_number() over(partition by id order by run_id) as rn from tempIndexTable) a where a.rn=1")
  }

  def readSchemaAndCreateTable(spark:SparkSession,indexTableName:String,baseTableName:String,tableLocation:String,
                               schemaFilePath:String,partitionColumn:String,primaryKeys:String)={

   var baseTableSchema = ""
    var indexTableSchema = ""
    var partCol = ""
    for ((nameOfCol,typeOfcol) <- parsingJson(schemaFilePath)) {
      if (!nameOfCol.equalsIgnoreCase(partitionColumn)) baseTableSchema+=nameOfCol+" "+typeOfcol+","
      else partCol+=nameOfCol+" "+typeOfcol
      if (primaryKeys.contains(nameOfCol)) indexTableSchema+=nameOfCol+" "+typeOfcol+","
    }

    //creating Base table.
    createHiveTable(spark,baseTableName,tableLocation,"base",baseTableSchema,partCol)
    //creating index table
    createHiveTable(spark,indexTableName,tableLocation,"index",indexTableSchema,"run_id int")
  }

  /**
    * parsingJson function will parse the json string and return a map of key and value.
    * @param schemaFilePath -- schema file path.
    * @return
    */
  def parsingJson(schemaFilePath:String)={
    val json = Source.fromFile(schemaFilePath)
    JSON.parseFull("")
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.readValue[Map[String, Object]](json.reader())
  }

}
