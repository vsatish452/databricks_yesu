# Databricks notebook source
dbutils.widgets.text("database", "SalesLT")

# COMMAND ----------

f = dbutils.widgets.get("database")

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/mnt/datapwc/rawlayer/"+ f + "/"))

# COMMAND ----------

l = ["Address","Customer","CustomerAddress","Product","ProductCategory","ProductDescription","ProductModel","ProductModelProductDescription","SalesOrderDetail","SalesOrderHeader"]

# COMMAND ----------

dbutils.widgets.dropdown("stateTbles", "Address", l)

# COMMAND ----------

table_name = dbutils.widgets.get("stateTbles")

# COMMAND ----------

df = spark.read.csv(path ="dbfs:/mnt/datapwc/rawlayer/"+ f + "/"+ table_name + "/",header = True)
display(df)

# COMMAND ----------


