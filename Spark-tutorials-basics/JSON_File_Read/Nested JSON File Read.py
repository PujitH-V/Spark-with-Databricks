# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

df_nested_json = spark.read.format("json").option("multiLine",True).load('/mnt/jnj/nested_json_example_dataset/*.json')

# COMMAND ----------

#Since the above file contains the data in a nested json format we would like to break and make it into flat json file or flat tabular data by removing the nested structure of JSON.

df_exploded = df_nested_json.select(explode(col('BrightspaceDataSets')).alias('exploded_brightspace_datasets'))
df_final = df_exploded.selectExpr("exploded_brightspace_datasets.*")
df_previous_datasets = df_final.select(explode('PreviousDataSets').alias('previous_ds'))
df_final1 = df_previous_datasets.selectExpr('previous_ds.*').drop('PreviousDataSets')
df_final = df_final.drop(col('PreviousDataSets'))
df_flatted_data = df_final.union(df_final1)

# COMMAND ----------

df_flatted_data.display()

# COMMAND ----------

df_flatted_data.write.mode('overwrite').format("delta").partitionBy("PluginId").save('/mnt/jnj/nested_json_flattened_output/DELTA/')

# COMMAND ----------

df_flatted_data.write.mode('overwrite').format("parquet").partitionBy("PluginId").bucketBy(2,"CreatedDate").saveAsTable('nested_json_flattened_table',path = '/mnt/jnj/nested_json_flattened_table/DELTA/')
