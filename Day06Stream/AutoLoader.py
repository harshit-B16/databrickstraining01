# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/hbadlshexawarelearning/raw/

# COMMAND ----------

(spark
.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.inferColumnTypes",True)
# .option("cloudFiles.schemaLocation","dbfs:/mnt/hexawaredatabricks/raw/schemalocation/harshit/autoloader")
.option("cloudFiles.schemaLocation", "dbfs:/mnt/hbadlshexawarelearning/raw/schemalocation/harshit/autoloader")
.load("dbfs:/mnt/hexawaredatabricks/raw/stream_in/")
.writeStream
.option("checkpointLocation","dbfs:/mnt/hbadlshexawarelearning/raw/checkpoint/harshit/autoloader")
.trigger(once=True)
.table("Databricksspace01.bronze.autoloader")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Databricksspace01.bronze.autoloader

# COMMAND ----------

# DBTITLE 1,Final
(spark
.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaEvolutionMode","addNewColumns")
.option("cloudFiles.schemaLocation","dbfs:/mnt/hexawaredatabricks/raw/schemalocation/harshit/autoloader")
.load("dbfs:/mnt/hexawaredatabricks/raw/stream_in/")
.writeStream
.option("checkpointLocation","dbfs:/mnt/hexawaredatabricks/raw/checkpoint/harshit/autoloader")
.option("mergeSchema",True)
.table("Databricksspace01.bronze.autoloader")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Databricksspace01.bronze.autoloader

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Databricksspace01.bronze.autoloader where Name = "Rony"

# COMMAND ----------


