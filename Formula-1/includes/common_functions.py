# Databricks notebook source
# MAGIC %md 
# MAGIC ###### This notbook contains the common functions which can be reused inside other notbooks

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

def add_ingestion_date(input_df):
  output_df = input_df.withColumn("ingestion_date", current_timestamp())
  return output_df