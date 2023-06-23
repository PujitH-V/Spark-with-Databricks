# Databricks notebook source
# MAGIC %python
# MAGIC #Each XML File contains data stored in the form of tags. 
# MAGIC #there are 2 main tags in every XML Document. 1.) RootTag => represents the very basic tag or strting tag which encolses/has all the data.
# MAGIC #2.) RowTag => which stores the actual data. Example: data is stored like records in a database table but in the form of xml tags in a XML document in this case.

# COMMAND ----------


dbutils.fs.ls('/mnt/jnj/XML_Sample_Data/')

# COMMAND ----------

df = spark.read.format("xml").option("rootTag","catalog").option("rowTag","book").load("/mnt/jnj/XML_Sample_Data/Sample-employee-XML-file.xml")

# COMMAND ----------

df.display()

# COMMAND ----------

df.coalesce(1).write.mode('overwrite').partitionBy("author").format("xml").save("/mnt/jnj/XML_Output_Fie/")

# COMMAND ----------


