# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest circuits.csv file

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

v_data_source

# COMMAND ----------

row_folder_path

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 1 : Create schema 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

Circuit_Schema = StructType(fields=[StructField("CircuitID", IntegerType(), False),
                                    StructField("CircuiteRef", StringType(),True),
                                    StructField("name", StringType(),True),
                                    StructField("location", StringType(),True),
                                    StructField("country", StringType(),True),
                                    StructField("lat", DoubleType(),True),
                                    StructField("lng", DoubleType(),True),
                                    StructField("alt", IntegerType(),True),
                                    StructField("url", StringType(),True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 1 : Read csv file using defined schema file 

# COMMAND ----------

#Read the circuits.csv file into dataframe
circuits_df = spark.read\
.option("header", True)\
.schema(Circuit_Schema)\
.csv(f"{row_folder_path}/circuits.csv")

# COMMAND ----------

# Select required columns only

from pyspark.sql.functions import col

circuits_selected_df = circuits_df.select(
    col("circuitId").alias("circuit_Id"),
    col("CircuiteRef"),
    col("name"),
    col("location"),
    col("country"),
    col("lat"),
    col("lng"),
    col("alt")
    )

# COMMAND ----------

# Rename some of the fields
cirecuits_renamed_df=circuits_selected_df.withColumnRenamed("circuitId","circuit_Id") \
.withColumnRenamed("circuitRef","circuit_Ref") \
.withColumnRenamed("lat","latitude") \
.withColumnRenamed("lng","longitude") \
.withColumnRenamed("alt","altitude")    

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

cirecuits_parameter_df = cirecuits_renamed_df.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# Add a column and populate current timestamp

#from pyspark.sql.functions import current_timestamp
#circuits_final_df = cirecuits_renamed_df.withColumn("ingestion_date", current_timestamp())  

#include the function from the common_functions notebook
circuits_final_df = add_ingestion_date(cirecuits_parameter_df)

# COMMAND ----------

# Write the result back to the datalake

circuits_final_df.write\
    .mode("overwrite") \
    .parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

Read_parq_df= spark.read.parquet(f"{processed_folder_path}/circuits/")

# COMMAND ----------

display(Read_parq_df)

# COMMAND ----------

dbutils.notebook.exit("Success")