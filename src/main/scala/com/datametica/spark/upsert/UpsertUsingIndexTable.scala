package com.datametica.spark.upsert

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object UpsertUsingIndexTable{

  /**
    *  load function basically does some transformation on data and creates table if don't exist then push the data to
    *  base table and index table.
    * @param spark
    * @param bucketLocation
    * @param dataFormat
    * @param tableLocation
    * @param tableName
    * @param schemaFilePath
    * @param partitionCol
    * @param key
    * @param versionColName
    * @param indexTableName
    */
  def loadData(spark:SparkSession, bucketLocation:String, dataFormat:String, tableLocation:String,
               tableName:String, schemaFilePath:String, partitionCol:String, key:String,
               versionColName:String, indexTableName:String)={

    import org.apache.spark.sql.functions._

    val readDataFromBucket = spark
      .read
      .format(dataFormat)
      .schema(
        Schema.createSchema(spark,schemaFilePath)
      ).csv(bucketLocation)
      .repartition(1)

    Schema.readSchemaAndCreateTable(spark,indexTableName,tableName,tableLocation,schemaFilePath,
      partitionCol,key,readDataFromBucket,versionColName)

    val dataFrameWithVersion = readDataFromBucket
      .withColumn(
        versionColName,
        lit(this.getVersionNumber()
        )
      )
    dataFrameWithVersion.write.mode(SaveMode.Append).insertInto(tableName)

    val changeDataForIndexTable = dataFrameWithVersion
      .selectExpr(
        key,versionColName
      ).withColumn(
        "run_id",lit(TableQuery.getRunId(spark,indexTableName,"next").toString)
      )

    val  latestDataFromIndexTable = TableQuery.getLatestPartitionDataFromIndex(spark,indexTableName)
        .repartition(1)

    val runId = TableQuery.getRunId(spark,indexTableName,"next").toString

    val newIndexData = this.generateNewDataForIndexTable(
      spark,
      changeDataForIndexTable,
      latestDataFromIndexTable,
      key
    ).withColumn("run_id",lit(runId)
    )

    newIndexData.write.mode(SaveMode.Append).insertInto(indexTableName)

  }

  /**
    * version number is just epoch time here.
    * @param spark
    * @param indexTableName
    * @return
    */
  def getVersionNumber()= (System.currentTimeMillis()/1000).toString

  /**
    * generateNewDataForIndexTable -- it will generate new data for index table by performing union
    * with new coming data and latest data from index table.
    *
    * @param spark
    * @param newSetOfData
    * @param latestDataFromIndexTable
    * @param key
    * @return
    */

  def generateNewDataForIndexTable(spark:SparkSession,newSetOfData:DataFrame,latestDataFromIndexTable:DataFrame,key:String)={
    val unionOfDataFrame = newSetOfData.union(latestDataFromIndexTable)
    import spark.implicits._
    val windowSpec = Window.partitionBy(key).orderBy('run_id desc)
    unionOfDataFrame.withColumn("row_number",row_number().over(windowSpec)).filter("row_number==1").drop("row_number")
  }

}

