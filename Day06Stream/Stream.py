# Databricks notebook source
# MAGIC %md
# MAGIC ### Structured Streaming

# COMMAND ----------

from pyspark.sql.types import *
users_schema=StructType([StructField("Id", IntegerType()),
                         StructField("Name", StringType()),
                         StructField("Gender", StringType()),
                         StructField("Salary", IntegerType()),
                         StructField("Country", StringType()),
                         StructField("Date", StringType())
])

# COMMAND ----------

(
spark
.readStream
.schema(users_schema)
.csv("dbfs:/mnt/hexawaredatabricks/raw/stream_in/",header=True)
.writeStream
.option("checkpointLocation","dbfs:/mnt/hexawaredatabricks/raw/checkpoint/Harshit/stream")
.trigger(once=True)
.table("Databricksspace01.bronze.stream")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Databricksspace01.bronze.stream

# COMMAND ----------

# 1. Schema Evolution
# 2. 1. new Column --- I should get notified
