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
    val spark = getSparkSession()

    BatchLoadUsingIndexTable.loadData(spark,inputFileLocation,"csv", tableLocation,
      baseTableName,schemaFilePath,partitionCol,primaryKey,versionColName,indexTableName)
  }

  def getSparkSession(): SparkSession = {
    val spark = SparkSession.builder().appName("UsertUsingIndexTable")
      .enableHiveSupport().getOrCreate()

    spark.sqlContext.setConf("hive.exec.dynamic.partition","true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.sqlContext.setConf("hive.metastore.try.direct.sql","false")
    spark.sqlContext.setConf("spark.sql.hive.manageFilesourcePartitions","false")
    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

}
