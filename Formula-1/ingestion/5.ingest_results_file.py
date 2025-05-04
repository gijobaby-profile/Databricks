# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest Result.json file ( single line json file)
# MAGIC 1. This JSON file is a single line JSON
# MAGIC 1. Read the results.json file
# MAGIC 1. Rename fields
# MAGIC 1. Add new fild
# MAGIC 1. Remove Status_id field
# MAGIC 1. Write the result dataframe into datalake in Parquet format with partitonBy(race_id)

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
# MAGIC ##### Step 1 : Read result.json file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType,FloatType


# COMMAND ----------

result_schema =  StructType(fields = [StructField("resultId", IntegerType(), False),
                            StructField("raceId", IntegerType(), True),
                            StructField("driverId", IntegerType(), True),
                            StructField("constructorId", IntegerType(), True),
                            StructField("number", IntegerType(), True),
                            StructField("grid", IntegerType(), True),
                            StructField("position", IntegerType(), True),
                            StructField("positionText", StringType(), True),
                            StructField("positionOrder", IntegerType(), True), 
                            StructField("points", FloatType(), True),
                            StructField("laps", IntegerType(), True),
                            StructField("time", StringType(), True),
                            StructField("miliseconds", FloatType(), True),
                            StructField("fastestLap", IntegerType(), True),
                            StructField("rank", IntegerType(), True),
                            StructField("fastestLapTime", StringType(), True),
                            StructField("fastestLapSpeed", FloatType(), True),  
                            StructField("statusId", IntegerType(), True)
                            
                            ])

# COMMAND ----------

result_df = spark.read \
    .schema(result_schema) \
    .json(f"{row_folder_path}/results.json")

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 2 : Rename and add fields

# COMMAND ----------

from pyspark.sql.functions import col, lit,current_timestamp

# COMMAND ----------

result_selected_df = result_df.select(
    col("resultId").alias("result_id"),
    col("raceId").alias("race_id"),
    col("driverId").alias("driver_id"),
    col("constructorId").alias("constructor_id"),
    col("number"),
    col("grid"),
    col("position"),
    col("positionText").alias("position_text"),
    col("positionOrder").alias("position_order"),
    col("points"),
    col("laps"),
    col("time"),
    col("Miliseconds").alias("milliseconds"),
    col("fastestLap").alias("fastest_lap"),
    col("rank"),
    col("fastestLapTime").alias("fastest_lap_time"),
    col("fastestLapSpeed").alias("fastest_lap_speed")
)

# COMMAND ----------

#results_final_df = result_selected_df.withColumn("ingestion_date", lit(current_timestamp()))

#include the function from the common_functions notebook
results_final_df = add_ingestion_date(result_selected_df)


# COMMAND ----------

#Add a new field data_source using widget at runtime
from pyspark.sql.functions import lit
results_final_df_1 = results_final_df.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3: Write into datalake in parquet format

# COMMAND ----------

results_final_df_1.write \
                .mode("overwrite") \
                .partitionBy("race_id") \
                .parquet(f"{processed_folder_path}/results")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/results"))

# COMMAND ----------

dbutils.notebook.exit("Success")