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

# COMMAND ----------

from pyspark.sql.functions import col, split, mean, when, desc
import matplotlib.pyplot as plt
%matplotlib inline

# COMMAND ----------

# Data Cleaning (Example for aitools)
# Check for missing value percentages
aitools.select(*[col(c) for c in aitools.columns]).summary().show()

# COMMAND ----------

# Handle missing values in Description 
df_aitools_clean = aitools.withColumn("Description", when(col("Description").isNull(), "NA").otherwise(col("Description")))

# COMMAND ----------

# Handle missing values in Charges (assuming numerical) by filling with median
df_aitools_clean = df_aitools_clean.withColumn("Charges", when(col("Charges").isNull(), df_aitools_clean.select(mean("Charges")).rdd.flatMap(lambda x: x).collect()[0]).otherwise(col("Charges")))


# COMMAND ----------

# Feature Engineering (aitools)
# Create a new binary "is_free" feature
df_aitools_clean = df_aitools_clean.withColumn("is_free", when(col("Free/Paid/Other") == "Free", True).otherwise(False))

# Split functionalities from "Useable For" (replace "," with your delimiter if different)
df_aitools_clean = df_aitools_clean.withColumn("Text_Analysis", split(col("Useable For"), ",").getItem(0).cast("boolean"))
df_aitools_clean = df_aitools_clean.withColumn("Image_Recognition", split(col("Useable For"), ",").getItem(1).cast("boolean"))


# COMMAND ----------

# Transformations for Visualization (aisurvey)
# Create income brackets (assuming income is numerical after cleaning)
income_brackets = when(col("`6. What is your approximate annual income(US$)?`") < 50000, "Low") \
                  .when(col("`6. What is your approximate annual income(US$)?`") >= 50000, 
                        when(col("`6. What is your approximate annual income(US$)?`") < 100000, "Medium") \
                        .otherwise("High"))

df_aisurvey_clean = aisurvey.withColumn("Income_Bracket", income_brackets)


# COMMAND ----------

# Handle missing values in Description (replace with your chosen strategy)
df_aitools_clean = aitools.withColumn("Description", when(col("Description").isNull(), "NA").otherwise(col("Description")))


# COMMAND ----------

df_aitools_clean.groupBy("Major Category").count().orderBy(desc("count")).display()


# COMMAND ----------

df_aitools_clean.groupBy("Major Category", "is_free").count().orderBy(desc("Major Category")).display()



# COMMAND ----------

# Assuming df_aitools_clean is your DataFrame with Major Category, is_free, and count columns

# Filter out rows containing URLs using like with escape character
df_filtered = df_aitools_clean.filter(~col("Major Category").like("%https://%"))

# Group by, sort, and display
df_filtered.groupBy("Major Category", "is_free").count().orderBy(desc("Major Category")).display()


