# Databricks notebook source
olist_dataset_list = dbutils.fs.ls('/mnt/jnj/olist/')

# COMMAND ----------

dataframe_details = {}

# COMMAND ----------

for i,j,k,l in olist_dataset_list:
    dataframe_details[j].replace(".","_")] = i


# COMMAND ----------

dataframe_details

# COMMAND ----------

for key,value in dataframe_details.items():
    exec(f"{key} = spark.read.csv('{value}',inferSchema=True,header=True)")

# COMMAND ----------

olist_customers_dataset_csv.display()

# COMMAND ----------

olist_customers_dataset_csv.printSchema()

# COMMAND ----------

olist_orders_dataset_csv.display()

# COMMAND ----------

olist_orders_dataset_csv.printSchema()

# COMMAND ----------

olist_order_payments_dataset_csv.display()

# COMMAND ----------

olist_order_payments_dataset_csv.printSchema()

# COMMAND ----------


