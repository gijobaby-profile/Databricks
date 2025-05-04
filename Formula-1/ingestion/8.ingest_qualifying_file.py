# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest qualifying.json file
# MAGIC ##### Loading multiple multiline json file in a folder

# COMMAND ----------

# MAGIC %md
# MAGIC #### Include the configuration notbook 

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md 
# MAGIC #### using widgets inorder to paramterize the data source at runtime

# COMMAND ----------

#dbutils.widgets.help()
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 : Read the Multiline JSON file using spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

qualifying_schema= StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                     StructField("raceId", IntegerType(), True),
                                     StructField("driverId", IntegerType(), True),
                                     StructField("constructorId", IntegerType(), True),
                                     StructField("number", IntegerType(), True),
                                     StructField("position", IntegerType(), True),
                                     StructField("q1", StringType(), True),
                                     StructField("q2", StringType(), True),
                                     StructField("q3", StringType(), True)
                                     ])

# COMMAND ----------

qualifying_df=spark.read \
    .schema(qualifying_schema) \
    .option("multiLine", True) \
    .json(f"{row_folder_path}/qualifying/qualifying_split_*.json")


# COMMAND ----------

display(qualifying_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 : Rename and add columns
# MAGIC 1. rename qualifyId , raceid , driverId, constructorId
# MAGIC 2. add ingestion_date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

qualifying_renamed_df = qualifying_df.withColumnRenamed('qualifyId', 'qualify_Id') \
    .withColumnRenamed('raceId', 'race_id') \
    .withColumnRenamed('driverId', 'driver_id') \
    .withColumnRenamed('constructorId', 'constructor_Id') \
    .withColumn("ingestion_date", current_timestamp())


# COMMAND ----------

#Add a new field data_source using widget at runtime
from pyspark.sql.functions import lit
qualifying_final_df = qualifying_renamed_df.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 : Write the output to the datalake in Parquet format

# COMMAND ----------

qualifying_final_df.write \
    .mode("overwrite") \
    .parquet(f"{processed_folder_path}/qualifying")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/qualifying"))

# COMMAND ----------

dbutils.notebook.exit("Success")