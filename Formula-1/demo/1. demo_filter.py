# Databricks notebook source
# MAGIC %run "../includes/configurations"

# COMMAND ----------

display(dbutils.fs.ls(f"{processed_folder_path}"))

# COMMAND ----------

race_df = spark.read.parquet(f"{processed_folder_path}/races/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Method 1 : filter using SQL methos where conditions

# COMMAND ----------

from pyspark.sql.functions import desc
race_filtered_df = race_df.filter("race_year in (2019, 2017) and name = 'Austrian Grand Prix' ") \
                          .orderBy(desc("race_year"))

# COMMAND ----------

from pyspark.sql.functions import desc, col
race_filtered_df = race_df.select(col("race_year"),col("race_id"), col("name")) \
    .filter("race_year in (2019, 2017) and name = 'Austrian Grand Prix' ") \
    .orderBy(desc("race_year"))

# COMMAND ----------

from pyspark.sql.functions import desc, col
race_filtered_df = race_df.select(col("race_year"),col("race_id"), col("name")) \
    .filter((col("race_year").isin (2019, 2017)) & (col("name") == 'Austrian Grand Prix') ) \
    .orderBy(col("race_year").desc())

# COMMAND ----------

# MAGIC %md
# MAGIC ### select statement ,  where condition ,  order by 

# COMMAND ----------

from pyspark.sql.functions import desc, col
race_filtered_df = race_df.select(col("race_year"),col("race_id"), col("name")) \
    .filter("race_year in (2019, 2017) and name = 'Austrian Grand Prix' ") \
    .orderBy(desc("race_year"))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Group by and having

# COMMAND ----------

from pyspark.sql.functions import col

# Group by race_year and count the number of races
race_count_df = race_filtered_df.groupBy("race_year").count()

# Filter results based on the count (equivalent to SQL's HAVING clause)
filtered_race_count_df = race_count_df.filter(col("count") > 1)

display(filtered_race_count_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Group by , aggregations and having

# COMMAND ----------

from pyspark.sql.functions import col, sum, count

# Group by race_year, and calculate the sum of race_id for each group
race_count_df = race_filtered_df.groupBy("race_year").agg(
    sum("race_id").alias("total_race_id"),  # Sum of race_id for each year
    count("*").alias("race_count")          # Count the number of rows (races) for each year
)

# Filter results based on the count (equivalent to SQL's HAVING clause)
filtered_race_count_df = race_count_df.filter(col("race_count") > 1)

display(filtered_race_count_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Method 2 : filter using python methos where conditions

# COMMAND ----------

from pyspark.sql.functions import col 
race_filtered_df = race_df.filter(col("race_year").isin(2019, 2017)).orderBy(col("race_year").asc())

# COMMAND ----------

display(race_filtered_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### window fuctions without partitionBy ( window function accorss entire dataset)

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

race_df= spark.read.parquet(f"{processed_folder_path}/races/").filter("race_year in (2019, 2017)")
display(race_df)

# COMMAND ----------

from pyspark.sql.functions import col, count
race_selected_df = race_df.select(col("race_id"), col("circuit_id"), col("name"), col("race_year")) 

# COMMAND ----------


    race_agg_df = race_selected_df.groupBy("race_year").agg( 
    count("*").alias("race_count") 
    )

                             
display(race_agg_df)