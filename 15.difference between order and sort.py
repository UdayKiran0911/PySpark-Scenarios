# Databricks notebook source
# MAGIC %md
# MAGIC ###### Order vs Sort
# MAGIC - SQL: <code>order by</code> and <code>sort by</code> are different:- order by will sort entire data, sort by by will do sorting by partition
# MAGIC - Python: <code>orderby</code> and <code>sort</code> does same as SQL <code>order by</code>, and <code>sortWithinPartitions</code> does what <code>sort by</code> does in SQL

# COMMAND ----------

dbutils.fs.put("/user/hive/warehouse/emp/emp1.csv","""CHANNEL_ID,CHANNEL_DESC,CHANNEL_CLASS,CHANNEL_CLASS_ID,CHANNEL_TOTAL,CHANNEL_TOTAL_ID
3,Direct Sales,Direct,12,Channel total,1
9,Tele Sales,Direct,12,Channel total,1
5,Catalog,Indirect,13,Channel total,1
4,Internet,Indirect,13,Channel total,1
2,Partners,Others,14,Channel total,1""", True)

dbutils.fs.put("/user/hive/warehouse/emp/emp2.csv","""CHANNEL_ID,CHANNEL_DESC,CHANNEL_CLASS,CHANNEL_CLASS_ID,CHANNEL_TOTAL,CHANNEL_TOTAL_ID
13,Direct Sales,Direct,12,Channel total,1
19,Tele Sales,Direct,12,Channel total,1
15,Catalog,Indirect,13,Channel total,1
14,Internet,Indirect,13,Channel total,1
12,Partners,Others,14,Channel total,1""", True)

# COMMAND ----------

from pyspark.sql.functions import spark_partition_id

# COMMAND ----------

#%fs rm -r "/user/hive/warehouse/emp/"

# COMMAND ----------

df = spark.read.csv("/user/hive/warehouse/emp", header=True).repartition(4,"CHANNEL_ID").withColumn("Partition_id", spark_partition_id())

# COMMAND ----------

display(df)

# COMMAND ----------

df.orderBy("CHANNEL_ID").show()

# COMMAND ----------

df.sort("CHANNEL_ID").show()

# COMMAND ----------

df.sortWithinPartitions("CHANNEL_ID").show()

# COMMAND ----------

df.write.format("delta").saveAsTable("df_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from df_data order by channel_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from df_data sort by channel_id;