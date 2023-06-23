-- Databricks notebook source
create database if not exists demo;

-- COMMAND ----------

create table if not exists demo.Metadata_Manager
(
  Table_Schema string,
  Table_Name string,
  Last_Run_CDC_Start_TS timestamp,
  Last_Run_CDC_End_TS timestamp,
  Active_Flag string,
  Load_Type string,
  last_run_Status string
)
using delta
location '/mnt/jnj/Metadata/Delta/'

-- COMMAND ----------

-- MAGIC %py
-- MAGIC
-- MAGIC dbutils.widgets.text("table_name",'')

-- COMMAND ----------

-- MAGIC %py
-- MAGIC
-- MAGIC # Configure JDBC connection properties
-- MAGIC jdbc_url = "jdbc:sqlserver://phvdevus.database.windows.net:1433;database=DEV_PHA_US_DB"
-- MAGIC
-- MAGIC table_name = dbutils.widgets.get("table_name")
-- MAGIC username = dbutils.secrets.get("dbricks-scope","phvdevus-admin-username")
-- MAGIC password = dbutils.secrets.get("dbricks-scope","phvdevus-admin-password")
-- MAGIC
-- MAGIC # Read data from the table into a DataFrame
-- MAGIC df = spark.read \
-- MAGIC     .format("jdbc") \
-- MAGIC     .option("url", jdbc_url) \
-- MAGIC     .option("query", "select TABLE_SCHEMA,TABLE_NAME from INFORMATION_SCHEMA.Tables where Table_Schema = 'demo'") \
-- MAGIC     .option("user", username) \
-- MAGIC     .option("password", password) \
-- MAGIC     .load()
-- MAGIC
-- MAGIC df.createOrReplaceTempView("metadata_manager")
-- MAGIC

-- COMMAND ----------

select * from metadata_manager

-- COMMAND ----------

insert into demo.Metadata_Manager values
('demo','Product_Price_Reference_Lookup','2005-06-18 15:20:19.427','2005-06-18 15:20:19.427','Yes','Full','SUCCESS'),
('demo','Transactions_Reference','2005-06-18 15:20:19.427','2005-06-18 15:20:19.427','Yes','Incr','SUCCESS'),
('demo','Manufacturer_Detail','2005-06-18 15:20:19.427','2005-06-18 15:20:19.427','Yes','Full','SUCCESS'),
('demo','All_Address_Details','2005-06-18 15:20:19.427','2005-06-18 15:20:19.427','Yes','incr','SUCCESS'),
('demo','customers','2005-06-18 15:20:19.427','2005-06-18 15:20:19.427','Yes','Incr','SUCCESS'),
('demo','Vendors','2005-06-18 15:20:19.427','2005-06-18 15:20:19.427','Yes','Full','SUCCESS'),
('demo','Products','2005-06-18 15:20:19.427','2005-06-18 15:20:19.427','Yes','Full','SUCCESS')

-- COMMAND ----------

select * from demo.Metadata_Manager

-- COMMAND ----------


