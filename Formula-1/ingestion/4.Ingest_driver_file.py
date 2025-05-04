# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest driver.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Include the configuration notbook 

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
# MAGIC #### Step 1 : Read the nested JSON file using reader dataframe API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

# As this is a nested JSON file first define the inner schema
name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)
])



# COMMAND ----------

# Now define the outer schema which includes the inner schema
drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)
])

# COMMAND ----------

driver_df=spark.read \
.schema(drivers_schema) \
.json(f"{row_folder_path}/drivers.json")

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Step 2: Rename columns and add new columns
# MAGIC 1. driverID rename to Driver_id
# MAGIC 1. driverRef rename to Driver_ref
# MAGIC 1. ingestion_date added
# MAGIC 1. name add by concatinate forename and surname

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, concat, lit

# COMMAND ----------

dirvers_with_column_df=driver_df.withColumnRenamed("driverId","driver_id") \
                                .withColumnRenamed("driverRef","driver_ref") \
                                .withColumn("ingestion_date", current_timestamp()) \
                                .withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname")))



# COMMAND ----------

# MAGIC %md 
# MAGIC ####Step 3: Drop the unwanted columns
# MAGIC 1. name.forname
# MAGIC 1. name.surname
# MAGIC 1. url

# COMMAND ----------

driver_final_df =dirvers_with_column_df.drop(col("url"))

# COMMAND ----------

#Add a new field data_source using widget at runtime
from pyspark.sql.functions import lit
driver_final_df_1 = driver_final_df.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4: Write the output to the data lake processed container in parquet format

# COMMAND ----------

driver_final_df_1.write \
               .mode("overwrite") \
               .parquet(f"{processed_folder_path}/drivers")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/drivers"))

# COMMAND ----------

dbutils.notebook.exit("Success")