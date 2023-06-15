# Databricks notebook source
dbutils.widgets.text("table_name","")

# COMMAND ----------

# Configure JDBC connection properties
jdbc_url = "jdbc:sqlserver://phvdevus.database.windows.net:1433;database=DEV_PHA_US_DB"

table_name = dbutils.widgets.get("table_name")
username = dbutils.secrets.get("dbricks-scope","phvdevus-admin-username")
password = dbutils.secrets.get("dbricks-scope","phvdevus-admin-password")

# Read data from the table into a DataFrame
df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", table_name) \
    .option("user", username) \
    .option("password", password) \
    .load()

df.createOrReplaceTempView("metadata_manager")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from metadata_manager

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create database if not exists PHV

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS PHV.Delta_Metadata_Manager 
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/jnj/Logs_and_Metadata/Metadata_Manager/Delta/'
# MAGIC AS
# MAGIC SELECT * FROM metadata_manager
# MAGIC

# COMMAND ----------

table_list = spark.sql("select concat(table_schema,'.',table_name) as table_name,table_load_type as Load_Type,table_new_updated_cdc_timestamp as CDC_TS from PHV.Delta_Metadata_Manager").collect()

# COMMAND ----------



# COMMAND ----------

dataframe_table_list = []
for i in table_list:
    exec(f"df_{i[0].replace('.','_')} = spark.read.format('jdbc').option('url', '{jdbc_url}').option('dbtable', '{i[0]}').option('user', '{username}').option('password', '{password}').load()")

# COMMAND ----------

df_dbo_Fall22_S001_7_Campaign.display()

# COMMAND ----------

df_dbo_Fall22_S001_7_Order_food_category.display()

# COMMAND ----------


