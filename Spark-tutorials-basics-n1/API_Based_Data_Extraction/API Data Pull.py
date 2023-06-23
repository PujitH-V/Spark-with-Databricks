# Databricks notebook source
import requests
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.fs.ls('/user/')

# COMMAND ----------

# MAGIC %debug
# MAGIC
# MAGIC class api_data_pull:
# MAGIC     def __init__(self,base_url):
# MAGIC         self.url = base_url
# MAGIC
# MAGIC     def pull_api_data(self,dataset_name):
# MAGIC         payload = {}
# MAGIC         headers = {}
# MAGIC         url = f"{self.url}{dataset_name}"
# MAGIC         response = requests.get(url,headers=headers,data=payload)
# MAGIC         result = response.json()
# MAGIC         df = spark.createDataFrame(data=result)
# MAGIC         df.write.mode('overwrite').parquet(f'/mnt/jnj/API_datasets/{dataset_name}/')
# MAGIC         print(f"Dataset Name : {dataset_name}")
# MAGIC         df.show(5)
# MAGIC
# MAGIC
# MAGIC #create class object and call the class method pull_api_data to pull data for individual datasets.
# MAGIC api_obj = api_data_pull(base_url = "https://jsonplaceholder.typicode.com/")
# MAGIC datasets_list = ["posts","comments","albums","photos","users","todos"]
# MAGIC
# MAGIC for i in datasets_list:
# MAGIC     api_obj.pull_api_data(i)

# COMMAND ----------


