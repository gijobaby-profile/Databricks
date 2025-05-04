# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Datalake containers for the project
# MAGIC 1. Create a function to mount and pass the storage_Account_Name and  container_names as parameter
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# Import necessary libraries
from pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,StringType

# COMMAND ----------

# define the schema
circuits_schema = StructType(fields=[StructField("circuitId",IntegerType(),False),
                                     StructField("circuitRef",StringType(),True),
                                     StructField("name",StringType(),True),
                                     StructField("location",StringType(),True),
                                     StructField("country",StringType(),True),
                                     StructField("lat",DoubleType(),True),
                                     StructField("lng",DoubleType(),True),
                                     StructField("alt",IntegerType(),True),
                                     StructField("url",StringType(),True)
])

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

# Read the datalake file content using dataframe API 
circuits_df = spark.read \
.option("header",True) \
.schema(circuits_schema) \
.csv("/mnt/gijoformula1dl/row/circuits.csv")

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 : Select only the required columns using col function

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df.select(
    col("circuitId"),
    col("circuitRef"),
    col("name"),
    col("location"),
    col("country"),
    col("lat"),
    col("lng"),
    col("alt")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 : Rename the fields using withColumnRenamed()

# COMMAND ----------

circuits_renamed_df=circuits_selected_df.withColumnRenamed("circuitId","circuit_Id") \
.withColumnRenamed("circuitRef","circuit_Ref") \
.withColumnRenamed("lat","latitude") \
.withColumnRenamed("lng","longitude") \
.withColumnRenamed("alt","altitude")     

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 : Adding new field using withColumn() function

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp())   

# COMMAND ----------

display(circuits_final_df)