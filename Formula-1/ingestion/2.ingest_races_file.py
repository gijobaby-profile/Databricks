# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest Races.csv file

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
# MAGIC ####Step 1 : Create schema 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType,DateType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                    StructField("year", IntegerType(),True),
                                    StructField("round", IntegerType(),True),
                                    StructField("circuitId", IntegerType(),True),
                                    StructField("name", StringType(),True),
                                    StructField("date", DateType(),True),
                                    StructField("time", StringType(),True),
                                    StructField("url", StringType(),True)
                                ])

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2 : Read csv file using defined schema file 

# COMMAND ----------

#Read the circuits.csv file into dataframe
races_df = spark.read\
.option("header", True)\
.schema(races_schema)\
.csv(f"{row_folder_path}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Add injection date and Race Timestamp to the dataframe
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col,lit, to_timestamp,concat

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp()) \
                                  .withColumn("race_timestamp",to_timestamp(concat(col("date"),lit(" "),col("time")),"yyyy-MM-dd HH:mm:ss"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4: Select only required columns and rename as required

# COMMAND ----------

# Select required columns only

races_selected_df = races_with_timestamp_df.select(
    col("raceid").alias("race_id"),
    col("year").alias("race_year"),
    col("round"),
    col("circuitid").alias("circuit_Id"),
    col("name"),
    col("race_timestamp"),
    col("ingestion_date")
    )

# COMMAND ----------

#Add a new field data_source using widget at runtime
from pyspark.sql.functions import lit
races_final_df = races_selected_df.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md 
# MAGIC #####Step 5: Write the data to the datalake using parquet file format

# COMMAND ----------

# Write the result back to the datalake

races_final_df.write\
    .mode("overwrite") \
    .partitionBy("race_year") \
    .parquet(f"{processed_folder_path}/races")

# COMMAND ----------

Read_parq_df= spark.read.parquet(f"{processed_folder_path}/races/")

# COMMAND ----------

display(Read_parq_df)

# COMMAND ----------

dbutils.notebook.exit("Success")