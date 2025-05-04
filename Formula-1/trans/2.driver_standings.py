# Databricks notebook source
# MAGIC %md
# MAGIC ## Driver_Standing
# MAGIC ###### This is to collect the data race_rsult dataset to develop a dataset, Driver_standing, for the website https://www.bbc.com/sport/formula1/standings

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

# MAGIC %md
# MAGIC #### To identify the total points and the number of wins per year for a driver

# COMMAND ----------

from pyspark.sql.functions import sum, count, col, when

# COMMAND ----------

driver_standing_df = race_results_df \
    .groupBy("race_year", "driver_name", "driver_nationality", "team") \
    .agg(sum("points").alias("total_points"),
         count(when(col("position") == 1, True)).alias("wins")
         )


# COMMAND ----------

display(driver_standing_df.orderBy(col("race_year"),col("wins").desc()))

# COMMAND ----------

display(driver_standing_df.filter("race_year = 2020").orderBy(col("wins").desc()))

# COMMAND ----------

# MAGIC %md 
# MAGIC #### To find the rank of each dirvers within the race year

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank
driver_rank_window_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
driver_rank_df = driver_standing_df.withColumn("rank",rank().over(driver_rank_window_spec))

# COMMAND ----------

display(driver_rank_df.filter("race_year = 2020"))

# COMMAND ----------

driver_rank_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standings")