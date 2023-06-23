# Databricks notebook source
dbutils.widgets.dropdown('file_format','parquet',['parquet','csv','json'])

# COMMAND ----------

from datetime import datetime

dt = datetime.now()

year = dt.year
month = dt.month
day = dt.day
hour = dt.hour

# COMMAND ----------

file_format = dbutils.widgets.get('file_format')

# COMMAND ----------

print(file_format)

# COMMAND ----------

Full_Load_Table_List = spark.sql("select concat(table_schema,'.',table_name)as table_name from demo.Metadata_Manager where Load_Type = 'Full' and Active_Flag = 'Yes'").collect()
Incr_Load_Table_List = spark.sql("select concat(table_schema,'.',table_name)as table_name,Last_Run_CDC_End_TS from demo.Metadata_Manager where Load_Type != 'Full' and Active_Flag = 'Yes'").collect()

# COMMAND ----------

# Configure JDBC connection properties
jdbc_url = "jdbc:sqlserver://phvdevus.database.windows.net:1433;database=DEV_PHA_US_DB"

username = dbutils.secrets.get("dbricks-scope","phvdevus-admin-username")
password = dbutils.secrets.get("dbricks-scope","phvdevus-admin-password")

# COMMAND ----------

Full_Load_Table_List

# COMMAND ----------

for i in Full_Load_Table_List:
    exec(f"df_{i[0].replace('.','_')} = spark.read.format('jdbc').option('url', '{jdbc_url}').option('dbtable', '{i[0]}').option('user', '{username}').option('password', '{password}').load()")
    if file_format == 'csv':
        exec(f"df_{i[0].replace('.','_')}.write.mode('overwrite').format('{file_format}').save('/mnt/jnj/SQL_DB_TO_ADLS_Datasets/{file_format}/Full_Load/{year}/{month}/{day}/{i[0]}/',header=True)")
    elif file_format == 'parquet':
        exec(f"df_{i[0].replace('.','_')}.write.mode('overwrite').format('{file_format}').save('/mnt/jnj/SQL_DB_TO_ADLS_Datasets/{file_format}/Full_Load/{year}/{month}/{day}/{i[0]}/')")

# COMMAND ----------

for i in Incr_Load_Table_List:
    exec(f"""df_{i[0][5:]} = spark.read.format('jdbc').option('url', '{jdbc_url}').option("query", "select * from {i[0]} where Record_Created_timestamp > '{i[1]}' ").option('user', '{username}').option('password', '{password}').load()""")
    exec(f"df_{i[0][5:]}.createOrReplaceTempView('{i[0][5:]}')")
    print("\n")
    if file_format == 'csv':
        exec(f"""df_{i[0][5:]}.write.mode('overwrite').format('{file_format}').save('/mnt/jnj/SQL_DB_TO_ADLS_Datasets/{file_format}/Incr_Load/{year}/{month}/{day}/{i[0]}/',header=True,sep='|')""")
    elif file_format == 'parquet':
        exec(f"""df_{i[0][5:]}.write.mode('overwrite').format('{file_format}').save('/mnt/jnj/SQL_DB_TO_ADLS_Datasets/{file_format}/Incr_Load/{year}/{month}/{day}/{i[0]}/')""")
    spark.sql(f"""update demo.metadata_manager set Last_Run_CDC_Start_TS = (select min(Record_Created_timestamp) from {i[0][5:]}), Last_Run_CDC_End_TS = (select max(Record_Created_timestamp) from {i[0][5:]}) where Table_Name = '{i[0]}'""")
