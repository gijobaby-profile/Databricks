# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Datalake using Service Principal
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 2. Generate a secret/ password for the Application
# MAGIC 3. Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret
# MAGIC 4. Assign Role Storage Blob Data Contributor to the Data Lake
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

client_id = dbutils.secrets.get(scope='gijoformula1-scope', key='gijoformula1-app-client-id')
tenent_id = dbutils.secrets.get(scope='gijoformula1-scope', key='gijoformula1-app-tenent-id')
client_secret = dbutils.secrets.get(scope='gijoformula1-scope', key='gijoformula1-app-client-secret')

# COMMAND ----------

#Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret

spark.conf.set("fs.azure.account.auth.type.gijoformula1dl.dfs.core.windows.net","OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.gijoformula1dl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.gijoformula1dl.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.gijoformula1dl.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.gijoformula1dl.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenent_id}/oauth2/token")

# COMMAND ----------

# List all the available files from Datalake
display(dbutils.fs.ls("abfss://demo@gijoformula1dl.dfs.core.windows.net"))


# COMMAND ----------

# Read the datalake file content using dataframe API 
display(spark.read.csv("abfss://demo@gijoformula1dl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

