# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Datalake using service principle
# MAGIC 1. Get clinet_id, tenent_id and clinet_secret from Azure key valult 
# MAGIC 2. Set spark configuration with App/client_id, Directory/tenent_id and secret
# MAGIC 3. call file system utility mount to mount the storage
# MAGIC 4. explore other filesystem utilities related to mount( list all mounts / unmounts)
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

client_id = dbutils.secrets.get(scope='gijoformula1-scope', key='gijoformula1-app-client-id')
tenent_id = dbutils.secrets.get(scope='gijoformula1-scope', key='gijoformula1-app-tenent-id')
client_secret = dbutils.secrets.get(scope='gijoformula1-scope', key='gijoformula1-app-client-secret')

# COMMAND ----------

#Creaete the Clientid, clinet secret and the tenent_id as a python dictionary
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenent_id}/oauth2/token"}


dbutils.fs.mount(
  source = "abfss://demo@gijoformula1dl.dfs.core.windows.net/",
  mount_point = "/mnt/gijoformula1dl/demo",
  extra_configs = configs)

# COMMAND ----------

# List all the available files from Datalake
display(dbutils.fs.ls("/mnt/gijoformula1dl/demo"))


# COMMAND ----------

# Read the datalake file content using dataframe API 
display(spark.read.csv("/mnt/gijoformula1dl/demo/circuits.csv"))

# COMMAND ----------



# COMMAND ----------

display(dbutils.fs.mounts())