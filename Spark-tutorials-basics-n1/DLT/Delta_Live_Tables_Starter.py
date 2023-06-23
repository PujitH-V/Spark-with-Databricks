# Databricks notebook source
spark

# COMMAND ----------

df = spark.read.json('/databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed-json/2015_2_clickstream.json')

# COMMAND ----------

df.display()

# COMMAND ----------


