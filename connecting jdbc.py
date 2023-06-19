# Databricks notebook source
# Import required libraries
from pyspark.sql import SparkSession

# Azure SQL Database connection details
server = 'skv-server.database.windows.net'
database = 'skvdatabase'
username = 'satish'
password = 'Rajesh@1143'
jdbc_url = f'jdbc:sqlserver://{server};database={database};user={username};password={password}'

# Create a SparkSession
spark = SparkSession.builder \
    .appName('AzureSQLRead') \
    .getOrCreate()

# Define the SQL query to fetch data from the database
query = 'SELECT * FROM SalesLT.Address'

# Read data from Azure SQL Database into a DataFrame
df = spark.read \
    .format('jdbc') \
    .option('url', jdbc_url) \
    .option('query', query) \
    .option('driver', 'com.microsoft.sqlserver.jdbc.SQLServerDriver') \
    .load()

# Perform operations on the DataFrame
df.show()


# COMMAND ----------

display(df.count())

# COMMAND ----------

df1 = df.dropDuplicates()

# COMMAND ----------

count = df1.count()

# COMMAND ----------

dbutils.notebook.exit(count)
