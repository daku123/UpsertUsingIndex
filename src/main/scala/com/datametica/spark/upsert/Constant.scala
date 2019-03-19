package com.datametica.spark.upsert

object Constant {

  val BASE_TABLE_CREATE_SYNTAX = "CREATE EXTERNAL TABLE IF NOT EXISTS tableName (schema) PARTITIONED BY " +
    "(partCol,version int) STORED AS PARQUET LOCATION 'baseTableLocation'"

  val INDEX_TABLE_CREATE_SYNTAX = "CREATE EXTERNAL TABLE IF NOT EXISTS tableName (schema,version int) " +
    "PARTITIONED BY (run_id string) LOCATION 'indexTableLocation' "
}
