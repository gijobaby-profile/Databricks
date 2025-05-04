# Databricks notebook source
# MAGIC %md
# MAGIC #### Explore the capabilities of dbutils.secrets utility
# MAGIC

# COMMAND ----------

dbutils.secrets.help()


# COMMAND ----------

dbutils.secrets.listScopes()


# COMMAND ----------

dbutils.secrets.list('gijoformula1-scope')

# COMMAND ----------

dbutils.secrets.get(scope='gijoformula1-scope', key='gijoformula1-account-key')