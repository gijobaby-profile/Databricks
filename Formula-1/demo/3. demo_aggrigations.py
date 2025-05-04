# Databricks notebook source
# MAGIC %run ../includes/configurations

# COMMAND ----------

# MAGIC %md
# MAGIC #### Simple Aggregations

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results") \
.filter("race_year in (2020,2019)")

# COMMAND ----------

from pyspark.sql.functions import count, sum,avg, countDistinct

# COMMAND ----------

display(race_results_df.select(count("*")))

# COMMAND ----------

display(race_results_df.select(countDistinct("race_name")))

# COMMAND ----------

race_results_df.select(sum("points").alias("pointsum")).show()



# COMMAND ----------

# MAGIC %md
# MAGIC #### Simple Aggregations with filters

# COMMAND ----------

race_results_df.filter("driver_name = 'Lewis Hamilton'") \
               .select(
                    sum("points").alias("Total_Points"), 
                    countDistinct("race_name").alias("race_count")
                ).show()




# COMMAND ----------

# MAGIC %md
# MAGIC #### Grouped Aggregations

# COMMAND ----------

from pyspark.sql.functions import desc,col

# COMMAND ----------

race_aggregated_df = race_results_df.groupBy("race_year", "driver_name") \
                                    .agg(
                                        sum("points").alias("Total_Points"), 
                                        countDistinct("race_name").alias("race_count")
                                    ).orderBy(col("Total_Points").desc())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Window functions
# MAGIC 1.  window need to import from pyspark.sql.window
# MAGIC 1.  Define a window specifications, by specifying the partitonBy fields and orderBy fields
# MAGIC 1.  Apply the window function eg Rank(), lead() , lag(), row_number()… into the dataframe using the defined window by adding a new column using withColumn() function

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# Define a window specification
rank_window_spec = Window.partitionBy("race_year").orderBy(desc("Total_Points"))

# Apply rank and show results
race_aggregated_df.withColumn("rank", rank().over(rank_window_spec)).show(100)
