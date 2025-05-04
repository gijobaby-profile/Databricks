# Databricks notebook source
# MAGIC %md
# MAGIC ##### To explore the DBFS Root
# MAGIC 1. list all the folder in DBFS Root
# MAGIC 2. intract with DBFS file browser
# MAGIC 3. upload files to DBFS root
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('/databricks-datasets/'))