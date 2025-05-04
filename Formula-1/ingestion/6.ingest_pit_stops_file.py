# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest Pit_stops.json file
# MAGIC ##### its a multiline json file

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

pit_stop_schema= StructType(fields=[StructField("raceId", IntegerType(), False),
                                     StructField("driverId", IntegerType(), True),
                                     StructField("stop", StringType(), True),
                                     StructField("lap", IntegerType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("duration", StringType(), True),
                                     StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

pit_stop_df=spark.read \
    .schema(pit_stop_schema) \
    .option("multiLine", True) \
    .json(f"{row_folder_path}/pit_stops.json")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 : Rename and add columns
# MAGIC 1. rename driverid and raceid
# MAGIC 2. add ingestion_date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

pit_stop_renamed_df = pit_stop_df.withColumnRenamed('driverId', 'driver_id') \
    .withColumnRenamed('raceId', 'race_id') \
    .withColumn("ingestion_date", current_timestamp())


# COMMAND ----------

#Add a new field data_source using widget at runtime
from pyspark.sql.functions import lit
pit_stop_final_df = pit_stop_renamed_df.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 : Write the output to the datalake in Parquet format

# COMMAND ----------

pit_stop_final_df.write \
    .mode("overwrite") \
    .parquet(f"{processed_folder_path}/pit_stops")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/pit_stops"))

# COMMAND ----------

dbutils.notebook.exit("Success")