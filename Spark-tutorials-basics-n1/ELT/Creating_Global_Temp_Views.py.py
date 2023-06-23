# Databricks notebook source
dbutils.widgets.text('read_file_format','parquet')
dbutils.widgets.text('ADLS_File_Paths','')

# COMMAND ----------

read_file_format = dbutils.widgets.get('read_file_format')

# COMMAND ----------

from datetime import datetime

dt = datetime.now()

Year = dt.year
Month = dt.month
Date = dt.day

# COMMAND ----------

incremental_datasets = dbutils.fs.ls(f'/mnt/jnj/SQL_DB_TO_ADLS_Datasets/parquet/Incr_Load/{str(Year)}/{str(Month)}/{str(Date)}/')
full_datasets = dbutils.fs.ls(f'/mnt/jnj/SQL_DB_TO_ADLS_Datasets/parquet/Full_Load/{str(Year)}/{str(Month)}/{str(Date)}/')
dataframe_details = {}

# COMMAND ----------

for i,j,k,l in incremental_datasets:
    dataframe_details[j.replace(".","_").replace("/","")] = i

for i,j,k,l in full_datasets:
    dataframe_details[j.replace(".","_").replace("/","")] = i

# COMMAND ----------

for key,value in dataframe_details.items():
    if read_file_format == 'csv':
        exec(f"{key} = spark.read.{read_file_format}('{value}',inferSchema=True,header=True).createOrReplaceGlobalTempView('{key}')")
        print(key)
    elif read_file_format == 'parquet':
        exec(f"{key} = spark.read.{read_file_format}('{value}').createOrReplaceGlobalTempView('{key}')")
        print(key)
