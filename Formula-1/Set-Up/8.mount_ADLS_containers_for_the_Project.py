# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Datalake containers for the project
# MAGIC 1. Create a function to mount and pass the storage_Account_Name and  container_names as parameter
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

#Create a function to mount and pass the storage_Account_Name and container_names as parameter
def mount_adls(storage_account_name, container_name):
    # get secrets from the key vault
    client_id = dbutils.secrets.get(scope='gijoformula1-scope', key='gijoformula1-app-client-id')
    tenent_id = dbutils.secrets.get(scope='gijoformula1-scope', key='gijoformula1-app-tenent-id')
    client_secret = dbutils.secrets.get(scope='gijoformula1-scope', key='gijoformula1-app-client-secret')

    # set spark configuration as a python dictionary
    configs = {"fs.azure.account.auth.type": "OAuth",
              "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
              "fs.azure.account.oauth2.client.id": client_id,
              "fs.azure.account.oauth2.client.secret": client_secret,
              "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenent_id}/oauth2/token"}

    # Check if the mount point is already mounted , if then need to unmount first 
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        print(f"Mount point /mnt/{storage_account_name}/{container_name} already exists, so need to unmount first")
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")    
    
    # mount the storage account container
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)  




# COMMAND ----------

for i in ['row','presentation','processed']:
  mount_adls('gijoformula1dl',i)


# COMMAND ----------

display(dbutils.fs.mounts())