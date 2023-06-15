# Databricks notebook source
import requests
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

class api_data_pull:
    def __init__(self,base_url):
        self.url = base_url

    def pull_api_data(self,dataset_name):
        payload = {}
        headers = {}
        url = f"{self.url}{dataset_name}"
        response = requests.get(url,headers=headers,data=payload)
        result = response.json()
        df = spark.createDataFrame(data=result)
        df.write.mode('overwrite').parquet(f'/mnt/jnj/API_datasets/{dataset_name}/')
        print(f"Dataset Name : {dataset_name}")
        df.show(5)


#create class object and call the class method pull_api_data to pull data for individual datasets.
api_obj = api_data_pull(base_url = "https://jsonplaceholder.typicode.com/")
datasets_list = ["posts","comments","albums","photos","users","todos"]

for i in datasets_list:
    api_obj.pull_api_data(i)

# COMMAND ----------


