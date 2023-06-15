# Databricks notebook source
# monotonically_increasing_id()
# row_number() (Window functions)
# crc32
# md5
# sha1 and sha2

# COMMAND ----------

df1 = spark.read.format("csv")\
.option("header", "true")\
.option("inferSchema", "true")\
.option("nullValue", "null")\
.load("dbfs:/FileStore/shared_uploads/udaykiran2487@gmail.com/emp.csv")

# COMMAND ----------

display(df1)

# COMMAND ----------

df2 = df1.drop("SAL","COMM","UPDATED_DATE")

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id, row_number, md5, sha2, crc32, lit, col
from pyspark.sql.window import Window

# COMMAND ----------

df2 = df2.withColumn("mii",monotonically_increasing_id())
display(df2)

# COMMAND ----------

df2 = df2.withColumn("wrn", row_number().over(Window.partitionBy(lit('')).orderBy(lit(''))))
display(df2)

# COMMAND ----------

df2 = df2.withColumn("crc32", crc32(df2["EMPNO"].cast("string")))
display(df2)

# crc32 can generate duplicates per 100K or 200K records

# COMMAND ----------

df2 = df2.withColumn("md5", md5(df2["EMPNO"].cast("string")))
display(df2)

# generates 32 bit hash key

# COMMAND ----------

df2 = df2.withColumn("sha2", sha2(df2["EMPNO"].cast("string"), 256))
display(df2)

# we can use 512, if there are 100 or 200 million records

# COMMAND ----------

# MAGIC %sql
# MAGIC select sha2("7369", 256)