# Databricks notebook source
df1 = spark.read.format("csv")\
.option("header", "true")\
.option("inferSchema", "true")\
.option("nullValue", "null")\
.load("dbfs:/FileStore/shared_uploads/udaykiran2487@gmail.com/emp.csv")

display(df1)

# COMMAND ----------

display(df1.filter("comm is null").count())

# COMMAND ----------

display(df1["EMPNO"].isNull())

# COMMAND ----------

from pyspark.sql.functions import count, when
df1.select([count(when(df1[i].isNull(),i)).alias(i) for i in df1.columns]).show()