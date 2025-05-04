# Databricks notebook source
# MAGIC %md
# MAGIC ##Race_Result 
# MAGIC
# MAGIC #### This is to collect the data from driver, constructor, race, result and circutis to develop a dataset for the website https://www.bbc.com/sport/formula1/2020/abu-dhabi-grand-prix/results

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers") \
    .withColumnRenamed("number", "driver_number") \
    .withColumnRenamed("name", "driver_name") \
    .withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors") \
    .withColumnRenamed("name", "team")

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
    .withColumnRenamed("location", "circuit_location")

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races") \
    .withColumnRenamed("name", "race_name") \
    .withColumnRenamed("race_timestamp", "race_date")

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results") \
    .withColumnRenamed("time", "race_time")


# COMMAND ----------

# MAGIC %md 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Based ond the ER diagram, results table have direct access to race, dreiver and constructor, but it is not directly connected with circuits. Circuit is connected with races. 
# MAGIC ##### Step 1 : So first we need to join races with circuits

# COMMAND ----------

race_circuit_df = races_df.join(circuits_df, races_df.circuit_Id == circuits_df.circuit_Id, "inner") \
    .select(races_df.race_id, races_df.race_name, races_df.race_year, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 : join the results_df with other dataframes

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

results_race_df = results_df.join(race_circuit_df, results_df.race_id == race_circuit_df.race_id, "inner") \
                            .join(drivers_df, results_df.driver_id == drivers_df.driver_id, "inner") \
                            .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id, "inner") \
                            

# COMMAND ----------

final_df = results_race_df.select("race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality", "team", "grid", "fastest_lap_speed", "race_time", "points","position", current_timestamp().alias("created_date"))


# COMMAND ----------

from pyspark.sql.functions import desc

# COMMAND ----------

display(final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")
dbutils.notebook.exit({"success": True})