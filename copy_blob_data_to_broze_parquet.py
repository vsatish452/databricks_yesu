# Databricks notebook source
display(dbutils.fs.ls("/mnt/transactions/"))

# COMMAND ----------

from pyspark.sql import functions as f
from pyspark.sql import SparkSession, DataFrame
from pyspark.dbutils import DBUtils
from pyspark.sql.window import Window
from pyspark.sql.types import *
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


#dbfs:/mnt/transactions/

df1 = spark.read.csv(path="dbfs:/mnt/transactions/",header = True,schema = schema).sort("trans_id")
df2 = df1.orderBy("trans_timestamp")
display(df2)

# COMMAND ----------


# adding addational columns

result_df = df2.withColumn("day", dayofmonth(col("trans_timestamp")))
result_df = result_df.withColumn("month", month(col("trans_timestamp")))
result_df = result_df.withColumn("year", year(col("trans_timestamp")))
result_df = result_df.withColumn("weekofyear", weekofyear(col("trans_timestamp")))
result_df = result_df.withColumn("months_in_words", date_format(col("trans_timestamp"), "MMMM"))
result_df = result_df.dropDuplicates()


# COMMAND ----------

display(result_df)

# COMMAND ----------

# Assuming you have a DataFrame called 'df' that you want to save as an external table
result_df.write.format('parquet').mode('append').option('path', 'dbfs:/mnt/datapwc/broze/transaction/').saveAsTable('adf_ext.transactions')



# COMMAND ----------

# Assuming you have a DataFrame called 'df' that you want to save as an external table
result_df.write.format('parquet').mode('overwrite').option('path', 'dbfs:/mnt/datapwc/broze/transaction/').saveAsTable('adf_ext.transactions1')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from adf_ext.transactions;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from adf_ext.transactions1;

# COMMAND ----------

 
