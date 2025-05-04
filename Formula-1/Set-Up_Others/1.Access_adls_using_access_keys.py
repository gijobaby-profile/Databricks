# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Datalake using Access Keys
# MAGIC 1. set the spark config fs.azure.account.key
# MAGIC 1. list files from demo container
# MAGIC 1. read data from circuits.csv file
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# To initilize the secret into a vairable
gijoformula1dl_account_key = dbutils.secrets.get(scope='gijoformula1-scope', key='gijoformula1-account-key')

# COMMAND ----------

#set the spark configuration and the access keys from the Azure
spark.conf.set("fs.azure.account.key.gijoformula1dl.dfs.core.windows.net", gijoformula1dl_account_key)

# COMMAND ----------

# List all the available files from Datalake
display(dbutils.fs.ls("abfss://demo@gijoformula1dl.dfs.core.windows.net"))


# COMMAND ----------

# Read the datalake file content using dataframe API 
display(spark.read.csv("abfss://demo@gijoformula1dl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

