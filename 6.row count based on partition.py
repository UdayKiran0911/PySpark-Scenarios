# Databricks notebook source
# MAGIC %fs ls /databricks-datasets/COVID/

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/COVID/USAFacts/

# COMMAND ----------

df = spark.read.option("header","true").csv("/databricks-datasets/COVID/USAFacts/*.csv")
display(df)

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

from pyspark.sql.functions import spark_partition_id
df.select(spark_partition_id().alias("part_id")).groupBy("part_id").count().show()

# COMMAND ----------

df.repartition(8).select(spark_partition_id().alias("part_id")).groupBy("part_id").count().show()