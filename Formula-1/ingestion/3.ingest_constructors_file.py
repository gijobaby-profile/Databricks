# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.JSON file 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Include the configuration notbook 

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md 
# MAGIC #### using widgets inorder to paramterize the data source at runtime

# COMMAND ----------

#dbutils.widgets.help()
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 : Read the JSON file using spark JSON dataframe reader API
# MAGIC ###### Here we are going to use DDL Formated string instead of StructType to define the schema.

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"



# COMMAND ----------

constructors_df=spark.read \
    .schema(constructors_schema) \
    .json(f"{row_folder_path}/constructors.json")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 : Remove the unwanted column

# COMMAND ----------


from pyspark.sql.functions import col


# COMMAND ----------

constructors_drop_df=constructors_df.drop(col('url'))


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 : Rename column and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

constructors_final_df=constructors_drop_df.withColumnRenamed('constructorId','constructor_id') \
                                          .withColumnRenamed('constructorRef','constructor_ref') 
                                         
 

# COMMAND ----------

#include the function from the common_functions notebook
circuits_final_df_1 = add_ingestion_date(constructors_final_df)

# COMMAND ----------

#Add a new field data_source using widget at runtime
from pyspark.sql.functions import lit
circuits_final_df_2 = circuits_final_df_1.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4: Write the final dataframe into datalake with parquet format and verify it

# COMMAND ----------

circuits_final_df_2.write.mode('overwrite').parquet(f"{processed_folder_path}/constructors")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/constructors"))

# COMMAND ----------

dbutils.notebook.exit("Success")