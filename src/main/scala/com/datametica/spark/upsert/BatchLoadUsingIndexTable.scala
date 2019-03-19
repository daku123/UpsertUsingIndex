package com.datametica.spark.upsert

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object BatchLoadUsingIndexTable {

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
      .config("spark.master", "local")
      .enableHiveSupport().getOrCreate()

  }

  def historyLoad(spark:SparkSession,bucketLocation:String,dataFormat:String,tableLocation:String,tableName:String)={

    import org.apache.spark.sql.functions._

    val readDataFromBucket = spark.read.format(dataFormat).option("inferschema","true").option("header","true").csv(bucketLocation)
    val version = (System.currentTimeMillis / 1000).toString
    createHiveTable(spark,tableName,tableLocation,dataFormat,readDataFromBucket,version)
    createIndexTable(spark,"db_gold.index",tableLocation)

    val dataFrameWithVersion = readDataFromBucket.withColumn("version",lit(getVersionNumber(spark,"db_gold.index")))
    dataFrameWithVersion.repartition(1).write.mode(SaveMode.Append).insertInto(tableName)

    val changeDataForIndexTable = dataFrameWithVersion.selectExpr("id","version").withColumn("run_id",lit(getRunId(spark,"db_gold.index","next").toString)).repartition(1)
    val  latestDataFromIndexTable = getLatestPartitionDatafromIndex(spark,"db_gold.index")
    val runId = getRunId(spark,"db_gold.index","next").toString
    val newIndexData = generateNewDataForIndexTable(spark,changeDataForIndexTable,latestDataFromIndexTable).withColumn("run_id",lit(runId))
    changeDataForIndexTable.show()
    latestDataFromIndexTable.show()
    newIndexData.show()
    newIndexData.repartition(1).write.mode(SaveMode.Append).insertInto("db_gold.index")
    //dataFrameWithVersion.selectExpr("id","version").write.mode(SaveMode.Append).insertInto("db_gold.index")
  }

  def createHiveTable(spark:SparkSession,tableName:String,tableLocation:String,format:String,dataFrame:DataFrame,version:String)={
    spark.sql(createTableSyntax(format,tableName,tableLocation,createSchemaForHiveTable(dataFrame)))
  }

  def createTableSyntax(format:String,tableName:String,tableLocation:String,schema:String):String={
    val baseTableLocation = tableLocation.concat("/baseTable")
    return s"CREATE EXTERNAL TABLE IF NOT EXISTS ${tableName} ( ${schema}) PARTITIONED BY (tmst String,version string) STORED AS PARQUET LOCATION '${baseTableLocation}'"
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
def getVersionNumber(spark:SparkSession,indexTableName:String):String={
  /*val vNumber = spark.sql(s"select coalesce(max(version),0) as v from ${indexTableName}")
  val version = vNumber.select("v").collect().map(_.getInt(0))
  version(0)+1*/
  (System.currentTimeMillis()/1000).toString
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
def createIndexTable(spark:SparkSession,indexTableName:String,tableLocation:String)={
  val indexTableLocation = tableLocation.concat("/indexTable")
  spark.sql(s"CREATE EXTERNAL TABLE IF NOT EXISTS ${indexTableName} ( id int,version string) PARTITIONED BY (run_id string) LOCATION '${indexTableLocation}' ")
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
}
