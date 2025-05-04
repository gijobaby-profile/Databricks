# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Datalake using SAS Tocken
# MAGIC 1. set the spark config fs.azure.account.key
# MAGIC 1. list files from demo container
# MAGIC 1. read data from circuits.csv file
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

#set the spark configuration and set the authentication type as SAS
spark.conf.set("fs.azure.account.auth.type.gijoformula1dl.dfs.core.windows.net","SAS")

#set the token provider type as FixedSASTokenPorvider from Azure
spark.conf.set("fs.azure.sas.token.provider.type.gijoformula1dl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")

#Generate and provide the SAS token for datalake ==> Go to Azure =>Storage Account =>  container => select the container we want to give access using SAS tocken => right side 3 dots ==> Generate SAS ==>  Permission as Read + list
spark.conf.set("fs.azure.sas.fixed.token.gijoformula1dl.dfs.core.windows.net", "sp=rl&st=2025-03-09T21:30:32Z&se=2025-03-10T05:30:32Z&spr=https&sv=2022-11-02&sr=c&sig=2ZbFiX%2BThBrDnuWKyEi7%2BmrVn1NfyIf51R4iXBjSWwA%3D")

# COMMAND ----------

# List all the available files from Datalake
display(dbutils.fs.ls("abfss://demo@gijoformula1dl.dfs.core.windows.net"))


# COMMAND ----------

# Read the datalake file content using dataframe API 
display(spark.read.csv("abfss://demo@gijoformula1dl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

