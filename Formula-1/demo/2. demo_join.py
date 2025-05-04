# Databricks notebook source
# MAGIC %run "../includes/configurations"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

race_df = spark.read.parquet(f"{processed_folder_path}/races").withColumnRenamed("name", "race_name").filter("race_year in (2018,2019,2020)")
display(race_df)

# COMMAND ----------

from pyspark.sql.functions import col, desc

circuit_df = spark.read.parquet(f"{processed_folder_path}/circuits").filter("circuit_id < 70").select("circuit_id", "location", "country")
#circuit_cnt_df = circuit_df.groupBy("country").count().orderBy(desc("count"))
display(circuit_df.orderBy(desc("circuit_id")))
#circuite_final_df = circuit_cnt_df


# COMMAND ----------

# MAGIC %md
# MAGIC ## Inner join

# COMMAND ----------

circuit_join_race_df = circuit_df.join(race_df,circuit_df.circuit_id == race_df.circuit_Id, "inner")
display(circuit_join_race_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Left join

# COMMAND ----------

circuit_join_race_df = circuit_df.join(race_df,circuit_df.circuit_id == race_df.circuit_Id, "left")
display(circuit_join_race_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Right join

# COMMAND ----------

circuit_join_race_df = circuit_df.join(race_df,circuit_df.circuit_id == race_df.circuit_Id, "right")
display(circuit_join_race_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Full Outer join

# COMMAND ----------

circuit_join_race_df = circuit_df.join(race_df,circuit_df.circuit_id == race_df.circuit_Id, "full") \
    .select (circuit_df.circuit_id, circuit_df.location, circuit_df.country, race_df.race_id, race_df.round, race_df.race_year)
display(circuit_join_race_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## semi join

# COMMAND ----------

circuit_join_race_df = circuit_df.join(race_df,circuit_df.circuit_id == race_df.circuit_Id, "semi") \
    .select (circuit_df.circuit_id, circuit_df.location, circuit_df.country ) #, race_df.race_id, race_df.round, race_df.race_year)
display(circuit_join_race_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Anti join
# MAGIC ####  an anti join is a type of join that returns rows from the left DataFrame that do not have matching rows in the right DataFrame based on a given condition.

# COMMAND ----------

circuit_join_race_df = circuit_df.join(race_df,circuit_df.circuit_id == race_df.circuit_Id, "anti") \
    .select (circuit_df.circuit_id, circuit_df.location, circuit_df.country ) #, race_df.race_id, race_df.round, race_df.race_year)
display(circuit_join_race_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cross join
# MAGIC #### cartition product

# COMMAND ----------

circuit_join_race_df = circuit_df.crossJoin(race_df)
display(circuit_join_race_df)

# COMMAND ----------

