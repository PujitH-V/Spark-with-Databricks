# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC select * from demo.metadata_manager

# COMMAND ----------

# MAGIC %sql
# MAGIC update demo.metadata_manager
# MAGIC set Last_Run_CDC_End_TS = '2005-06-18T15:20:19.427'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo.metadata_manager

# COMMAND ----------


