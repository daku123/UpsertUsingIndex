package com.datametica.spark.upsert

import org.apache.spark.sql.SparkSession

object MainClass {

  def main(args: Array[String]): Unit = {

    val baseTableName = args(0)
    val indexTableName = args(1)
    val primaryKey = args(2)
    val partitionCol = args(3)
    val versionColName = args(4)
    val tableLocation = args(5)
    val inputFileLocation = args(6)
    val schemaFilePath = args(7)
    val numberOfPartitions = args(8).toInt
    val spark = if (args.length>9)getSparkSession(args(9)) else getSparkSession()

    UpsertUsingIndexTable.loadData(spark,inputFileLocation,"csv", tableLocation,
      baseTableName,schemaFilePath,partitionCol,primaryKey,versionColName,indexTableName,numberOfPartitions)
  }

  /**
    *  creates spark session and add some of the hive-properties to it.
    * @return
    */
  def getSparkSession(numberOfSuffle:String="10"): SparkSession = {
    val spark = SparkSession.builder().appName("UsertUsingIndexTable")
      .enableHiveSupport().getOrCreate()

    spark.sqlContext.setConf("hive.exec.dynamic.partition","true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.sqlContext.setConf("hive.metastore.try.direct.sql","false")
    spark.sqlContext.setConf("spark.sql.hive.manageFilesourcePartitions","false")
    spark.sqlContext.setConf("spark.sql.shuffle.partitions",numberOfSuffle)
    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

}
