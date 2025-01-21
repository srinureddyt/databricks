# Databricks notebook source
dbutils.fs.rm('dbfs:/user/hive/warehouse/stream.db',True)
dbutils.fs.rm('dbfs:/FileStore/streaming',True)

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

source_dir = 'dbfs:/FileStore/streaming/'

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading the streaming dataframe

# COMMAND ----------

df = spark.readStream.format("csv")\
        .option('header','true')\
        .schema(schema)\
        .load(source_dir)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### 01. Trigger - default or unspecifed Trigger

# COMMAND ----------

 WriteStream = ( df.writeStream
        .option('checkpointLocation',f'{source_dir}/AppendCheckpoint')
        .outputMode("append")
        .queryName('DefaultTrigger')
        .toTable("stream.AppendTable"))

# COMMAND ----------



# COMMAND ----------

 WriteStream = ( df.writeStream
        .option('checkpointLocation',f'{source_dir}/AppendCheckpoint')
        .outputMode("append")
        .trigger(availableNow=True)
        .queryName('AvailableNow')
        .toTable("stream.AppendTable"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM stream.AppendTable

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table stream.AppendTable

# COMMAND ----------


