# Databricks notebook source
dbutils.widgets.text('App_Id','')
dbutils.widgets.dropdown('base_currency','USD',['USD','INR','CAD'])

# COMMAND ----------

Application_Id = dbutils.secrets.get(scope = "dbricks-scope", key = 'openexchange-appid')
Base_Currency = dbutils.widgets.get('base_currency')

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
import requests
import json

response = requests.get('https://openexchangerates.org/api/latest.json?app_id=08c73b285ad44d2f91b01c776234a557&base=USD')
data = response.json()

data_dict = {}
for currency, rate in data['rates'].items():
    data_dict[currency] = rate

data = [(k, float(v)) for k, v in data_dict.items()]
schema = StructType([
    StructField("Currency", StringType(), True),
    StructField("Rate", DoubleType(), True)
])
df = spark.createDataFrame(data, schema)
df = df.withColumn('Base_Currency_Code',lit('USD')).withColumnRenamed('Currency','Target_Currency_Code').withColumnRenamed('Rate','Conversion_Rate_Today')
df.createOrReplaceTempView('currency_xref')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC truncate table demo.Dim_Country_currency_conversion_xref

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into demo.Dim_Country_currency_conversion_xref (Base_Currency_Code, Target_Currency_Code, Conversion_Rate_Today)
# MAGIC select S.Base_Currency_Code,S.Target_Currency_Code,S.Conversion_Rate_Today from currency_xref S

# COMMAND ----------


