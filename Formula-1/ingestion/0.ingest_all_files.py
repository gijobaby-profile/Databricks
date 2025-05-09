# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

v_result = dbutils.notebook.run("1.ingest_circuits_file", 0, {"p_data_source": "Ergast_API"})

if (v_result == "Success"):
  v_result = dbutils.notebook.run("2.ingest_races_file", 0, {"p_data_source": "Ergast_API"}) 
else:
    import sys; sys.exit(0)
  

if (v_result == "Success"):
  v_result = dbutils.notebook.run("3.ingest_constructors_file", 0, {"p_data_source": "Ergast_API"}) 
else:
    import sys; sys.exit(0)


if (v_result == "Success"):
  v_result = dbutils.notebook.run("4.Ingest_driver_file", 0, {"p_data_source": "Ergast_API"}) 
else:
    import sys; sys.exit(0)


if (v_result == "Success"):
  v_result = dbutils.notebook.run("5.ingest_results_file", 0, {"p_data_source": "Ergast_API"}) 
else:
    import sys; sys.exit(0)

if (v_result == "Success"):
  v_result = dbutils.notebook.run("6.ingest_pit_stops_file", 0, {"p_data_source": "Ergast_API"}) 
else:
    import sys; sys.exit(0)

if (v_result == "Success"):
  v_result = dbutils.notebook.run("7.ingest_lap_times_file", 0, {"p_data_source": "Ergast_API"}) 
else:
    import sys; sys.exit(0)

if (v_result == "Success"):
  v_result = dbutils.notebook.run("8.ingest_qualifying_file", 0, {"p_data_source": "Ergast_API"}) 
else:
    import sys; sys.exit(0)
    
dbutils.notebook.exit("Success")

# COMMAND ----------

