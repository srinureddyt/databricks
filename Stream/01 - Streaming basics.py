# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType , IntegerType, FloatType

schema = StructType([   
                     StructField('Country',StringType()),
                     StructField('Citizens',IntegerType())
])

# COMMAND ----------

source_dir = 'dbfs:/FileStore/streaming/'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS  stream;
# MAGIC use stream

# COMMAND ----------

df = spark.readStream.format("csv")\
        .option('header','true')\
        .schema(schema)\
        .load(source_dir)

# COMMAND ----------

display(df)

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

WriteStream.stop()

# COMMAND ----------


