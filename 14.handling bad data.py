# Databricks notebook source
# MAGIC %md
# MAGIC ###### Bad Data
# MAGIC - Missing information
# MAGIC - Incomplete information
# MAGIC - Schema mismatch
# MAGIC - Differing formats or data types
# MAGIC - User errors when writing data producers

# COMMAND ----------

dbutils.fs.put("/FileStore/tables/channels.csv","""CHANNEL_ID,CHANNEL_DESC,CHANNEL_CLASS,CHANNEL_CLASS_ID,CHANNEL_TOTAL,CHANNEL_TOTAL_ID
3,Direct Sales,Direct,12,Channel total,1
9,Tele Sales,Direct,12,Channel total,1
5,Catalog,Indirect,13,Channel total,1
4,Internet,Indirect,13,Channel total,1
2,Partners,Others,14,Channel total,1
12,Partners,Others,14,Channel total,1,45,ram,3434
sample,Partners,Others,14,Channel total,1,45,ram,3434
10 Partners Others 14 Channel total 1
11 Partners Others 14 Channel total 1""", True)

# COMMAND ----------

df = spark.read.csv("/FileStore/tables/channels.csv", header=True)
display(df)

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, StructType, StructField

data_schema = StructType(fields = [
    StructField('CHANNEL_ID', IntegerType()),
    StructField('CHANNEL_DESC', StringType()),
    StructField('CHANNEL_CLASS', StringType()),
    StructField('CHANNEL_CLASS_ID', IntegerType()),
    StructField('CHANNEL_TOTAL', StringType()),
    StructField('CHANNEL_TOTAL_ID', IntegerType())
])

df = spark.read.csv("/FileStore/tables/channels.csv", header=True, schema=data_schema)

# COMMAND ----------

display(df)

# COMMAND ----------

# Allow bad data "PERMISSIVE" (default)
df1 = spark.read.option("mode","PERMISSIVE").csv("/FileStore/tables/channels.csv", header=True, schema=data_schema)
display(df1)

# COMMAND ----------

# Raise Expception for bad data "FAILFAST"
display(spark.read.option("mode","FAILFAST").csv("/FileStore/tables/channels.csv", header=True, schema=data_schema))

# COMMAND ----------

# Drop bad data "DROPMALFORMED"
display(spark.read.option("mode","DROPMALFORMED").csv("/FileStore/tables/channels.csv", header=True, schema=data_schema))

# COMMAND ----------

# store bad record details
df2 = spark.read.option("badRecordsPath","/channels/badrecords/").csv("/FileStore/tables/channels.csv", header=True, schema=data_schema)
display(df2)

# COMMAND ----------

# MAGIC %fs head "/channels/badrecords/20230609T083958/bad_records/part-00000-8fc05fc5-9227-4a9b-9e31-d2771ef76fbb"

# COMMAND ----------

# add badrecord details as a column
df_channels = spark.read.schema(schema_channels).csv("/FileStore/tables/channels.csv",header=True,columnNameOfCorruptRecord="BadData")
display(df_channels)

# COMMAND ----------

data_schema = StructType(fields = [
    StructField('CHANNEL_ID', IntegerType()),
    StructField('CHANNEL_DESC', StringType()),
    StructField('CHANNEL_CLASS', StringType()),
    StructField('CHANNEL_CLASS_ID', IntegerType()),
    StructField('CHANNEL_TOTAL', StringType()),
    StructField('CHANNEL_TOTAL_ID', IntegerType()),
    StructField('BadData', StringType())
])


df_channels = spark.read.schema(data_schema).csv("/FileStore/tables/channels.csv",header=True,columnNameOfCorruptRecord="BadData")
display(df_channels)

# COMMAND ----------

goodData = df_channels.filter(df_channels["BadData"].isNull())
display(goodData)

# COMMAND ----------

badData = df_channels.filter(df_channels["BadData"].isNotNull())
display(badData)