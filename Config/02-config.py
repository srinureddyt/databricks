# Databricks notebook source

# Establish connection to required external databases

snowflake_config = {
  "sfURL" : "wmwtiei-xbb77491.snowflakecomputing.com",
  "sfUser" : "HARITHA82",
  "sfPassword" : "Sanjana@10",
  "sfDatabase" : "SNOWFLAKE_SAMPLE_DATA",
  "sfSchema" : "TPCDS_SF100TCL",
  "sfWarehouse" : "COMPUTE_WH"
}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

try:

    # Database connection check
  df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
  .options(**snowflake_config) \
  .option("query",  "select * from SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CALL_CENTER") \
  .load()

  print("Database connection successful!")
except Exception as e:
    print("Database connection failed:", e)
