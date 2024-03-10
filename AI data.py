# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType

# COMMAND ----------

dbutils.fs.mount(
    source='wasbs://ai-data@moviesdatasa.blob.core.windows.net',
    mount_point='/mnt/ai-data',
    extra_configs = {'fs.azure.account.key.moviesdatasa.blob.core.windows.net': dbutils.secrets.get('moviesScopeProject', 'storageAccountKey')}
)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/ai-data"

# COMMAND ----------

aitools = spark.read.format("csv").option("header","true").load("/mnt/ai-data/raw-data/all_ai_tool.csv")
aisurvey = spark.read.format("csv").option("header","true").load("/mnt/ai-data/raw-data/Perceptions_towards_AI_Survey.csv")

# COMMAND ----------

aitools.show()

# COMMAND ----------

aisurvey.show()

# COMMAND ----------

aitools.printSchema()

# COMMAND ----------

aisurvey.printSchema()
