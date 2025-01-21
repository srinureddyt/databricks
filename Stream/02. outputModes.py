# Databricks notebook source
dbutils.fs.rm('dbfs:/user/hive/warehouse/stream.db',True)
dbutils.fs.rm('dbfs:/FileStore/streaming',True)

# COMMAND ----------

dbutils.fs.rm('dbfs:/FileStore/streaming/CompleteCheckpoint',True)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS stream CASCADE;
# MAGIC CREATE DATABASE IF NOT EXISTS stream

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType , IntegerType, FloatType

schema = StructType([   
                     StructField('Country',StringType()),
                     StructField('Citizens',IntegerType())
])

# COMMAND ----------

source_dir = 'dbfs:/FileStore/streaming/'

# COMMAND ----------

df = spark.readStream.format("csv")\
        .option('header','true')\
        .schema(schema)\
        .load(source_dir)

# COMMAND ----------

 WriteStream = ( df.writeStream
        .option('checkpointLocation',f'{source_dir}/AppendCheckpoint')
        .outputMode("append")
        .queryName('AppendQuery')
        .toTable("stream.AppendTable"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM stream.AppendTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Complete

# COMMAND ----------

from pyspark.sql.functions import sum
df_complete = df.groupBy('Country').agg(sum('Citizens').alias('Total_Population'))

# COMMAND ----------

 WriteCompleteStream = ( df_complete.writeStream
        .option('checkpointLocation',f'{source_dir}/CompleteCheckpoint')
        .outputMode("complete")
        .queryName('CompleteQuery')
        .toTable("stream.CompleteTable"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM stream.CompleteTable

# COMMAND ----------


