# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest lap times multiple CSV files
# MAGIC ##### Multiple CSV files in a folder

# COMMAND ----------

# MAGIC %md
# MAGIC #### Include the configuration notbook 
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %md 
# MAGIC #### using widgets inorder to paramterize the data source at runtime

# COMMAND ----------

#dbutils.widgets.help()
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 : Read the Multiple CSV file using spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

lap_times_schema= StructType(fields=[StructField("raceId", IntegerType(), False),
                                     StructField("driverId", IntegerType(), True),
                                     StructField("lap", IntegerType(), True),
                                     StructField("position", IntegerType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

lap_times_df=spark.read \
    .schema(lap_times_schema) \
    .csv(f"{row_folder_path}/lap_times/lap_times_*.csv")


# COMMAND ----------

display(lap_times_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 : Rename and add columns
# MAGIC 1. rename driverid and raceid
# MAGIC 2. add ingestion_date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

lap_times_renamed_df = lap_times_df.withColumnRenamed('driverId', 'driver_id') \
    .withColumnRenamed('raceId', 'race_id') \
    .withColumn("ingestion_date", current_timestamp())


# COMMAND ----------

#Add a new field data_source using widget at runtime
from pyspark.sql.functions import lit
lap_times_final_df = lap_times_renamed_df.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 : Write the output to the datalake in Parquet format

# COMMAND ----------

lap_times_final_df.write \
    .mode("overwrite") \
    .parquet(f"{processed_folder_path}/lap_times")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/lap_times"))

# COMMAND ----------

dbutils.notebook.exit("Success")