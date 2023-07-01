# Databricks notebook source
display(dbutils.fs.ls("/mnt/transactions/"))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/mnt/transactions/2023-06-29T09:00:00/"))

# COMMAND ----------

from pyspark.sql import functions as f
from pyspark.sql import SparkSession, DataFrame
from pyspark.dbutils import DBUtils
from pyspark.sql.window import Window
from pyspark.sql.types import 
from datetime import datetime, timezone
from pyspark.sql.types import StructType, IntegerType, StringType, LongType
from pyspark.sql.functions import *

# COMMAND ----------

# Define the schema using StructType and StructField
schema = StructType([
    StructField("trans_id", StringType(), nullable=False),
    StructField("trans_amount", DoubleType(), nullable=False),
    StructField("trans_timestamp", TimestampType(), nullable=False)
])

# COMMAND ----------


#reading data from blod storage

df = spark.read.csv(path="dbfs:/mnt/transactions/2023-06-29T09:00:00/",header = True,schema = schema)
display(df)

# COMMAND ----------


#dbfs:/mnt/transactions/

df1 = spark.read.csv(path="dbfs:/mnt/transactions/",header = True,schema = schema)
display(df1)

# COMMAND ----------


