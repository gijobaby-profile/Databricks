# Databricks notebook source
# MAGIC %md
# MAGIC ## Access dataframes using SQL
# MAGIC 1. Create a temporary view on a dataframe
# MAGIC 1. Access the view from the SQL cess
# MAGIC 1. Access the view from python cell

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

race_result_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

# MAGIC %md
# MAGIC #### To create a temporary view

# COMMAND ----------

race_result_df.createOrReplaceTempView("v_race_result")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from v_race_result where race_year =2020

# COMMAND ----------

# MAGIC %md 
# MAGIC #### how to run SQL from python block
# MAGIC

# COMMAND ----------

result_df = spark.sql("select * from v_race_result where race_year = 2020")

# COMMAND ----------

display(result_df)