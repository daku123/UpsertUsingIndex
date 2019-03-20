package com.datametica.spark.upsert

import org.apache.spark.sql.SparkSession

object TableQuery {

  /**
    * createHiveTable basically creates hive tables both base table and index table.
    * @param spark
    * @param tableName
    * @param tableLocation
    * @param tableType
    * @param schema
    * @param partCol
    * @param versionCol
    * @return
    */
  def createHiveTable(spark:SparkSession,tableName:String,tableLocation:String,
                      tableType:String,schema:String,partCol:String,versionCol:String)={

    val baseTableLocation = tableLocation.concat("/baseTable")
    val indexTableLocation = tableLocation.concat("/indexTable")

    if (tableType.equalsIgnoreCase("base"))
      spark.sql(s"CREATE EXTERNAL TABLE IF NOT EXISTS $tableName ($schema) PARTITIONED BY " +
        s"($partCol,$versionCol string) STORED AS PARQUET LOCATION '$baseTableLocation'")

    if (tableType.equalsIgnoreCase("index"))
      spark.sql(s"CREATE EXTERNAL TABLE IF NOT EXISTS $tableName ( $schema,$versionCol string) " +
        s"PARTITIONED BY (run_id string) LOCATION '$indexTableLocation' ")

  }

  /**
    * get run-id from index table.
    * @param spark
    * @param indexTableName
    * @return
    */
  def getRunId(spark:SparkSession,indexTableName:String,typeOfRunId:String):Int={
    val runIdDataFrame = spark.sql(s"select coalesce(max(run_id),0) as run_id from $indexTableName")
    val maxRunId = runIdDataFrame.select("run_id").collect().map(_.getString(0))
    if (typeOfRunId.equalsIgnoreCase("next"))
      maxRunId(0).toInt+1
    else maxRunId(0).toInt
  }

  /**
    * get all the data of latest partitions from index table.
    * @param spark
    * @param indexTableName
    * @return
    */
  def getLatestPartitionDataFromIndex(spark:SparkSession,indexTableName:String)={
    val maxRunId = this.getRunId(spark,indexTableName,"max").toString
    spark.table(indexTableName).where(s"run_id==$maxRunId")
  }

}
