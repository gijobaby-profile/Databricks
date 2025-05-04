# Databricks notebook source
# MAGIC %md
# MAGIC ## Constructor Standing
# MAGIC ###### This is to collect the data from race_result dataset to develop a dataset, constructor_standing, for the website https://www.bbc.com/sport/formula1/standings

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Find the team total points and total wins

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, col, desc, rank, avg

# COMMAND ----------

constructor_standing_df = race_results_df.groupBy("race_year","team").agg(
                                                                        sum("points").alias("total_points"),
                                                                        count(when(col("position") == 1, True)).alias("wins")
                                                                        )


# COMMAND ----------

# MAGIC %md
# MAGIC #### Rank the team based on the total_points and the wins in each year

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc

Constructor_window_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
constructor_standing_rank_df = constructor_standing_df.withColumn("rank",rank().over(Constructor_window_spec))

# COMMAND ----------

display(constructor_standing_rank_df.filter("race_year = 2020").orderBy(desc("total_points")))

# COMMAND ----------

constructor_standing_rank_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructor_standings")