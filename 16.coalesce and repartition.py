# Databricks notebook source
# MAGIC %md 
# MAGIC ###### Coalesce and repartition
# MAGIC - Coalesce: 
# MAGIC   - will not shuffle during partition
# MAGIC   - Can be used during performance tuning
# MAGIC   - can only decrease the existing partitions
# MAGIC   - No Shuffle Read or Shuffle Write
# MAGIC   - Setting Shuffle=True (default is False), will behave as repartition
# MAGIC   - Default is Narrow transformation
# MAGIC - repartition: will shuffle shuffle during partition
# MAGIC   - will shuffle during partition
# MAGIC   - can be used to decrease/increase the partitions
# MAGIC   - does Shuffle Read and Shuffle Write
# MAGIC   - Default is Wide transformation

# COMMAND ----------

rdd = sc.parallelize(range(10), 4)

# COMMAND ----------

print(rdd.glom().collect())

# COMMAND ----------

rdd1 = rdd.coalesce(2)
rdd2 = rdd.repartition(2)

# COMMAND ----------

print("Original: ",rdd.glom().collect())
print("Coalesce: ", rdd1.glom().collect())
print("Repartition: ",rdd2.glom().collect())

# COMMAND ----------

rdd1 = rdd.coalesce(2, shuffle=True)
rdd2 = rdd.repartition(2)

print("Original: ",rdd.glom().collect())
print("Coalesce: ", rdd1.glom().collect())
print("Repartition: ",rdd2.glom().collect())

# COMMAND ----------

# on dataframes
# import file for below example (File>>Upload File to DBFS)
df = spark.read.csv("dbfs:/FileStore/sourcefiles/emp-1.csv", header=True)
display(df)

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

from pyspark.sql.functions import spark_partition_id
df1 = df.repartition(4).withColumn("Part_id", spark_partition_id())
display(df1)

# COMMAND ----------

df1.rdd.getNumPartitions()

# COMMAND ----------

df2 = df.coalesce(4).withColumn("Part_id", spark_partition_id()) # shuffle option not avaialbe in Datafarme, so it will only works for decreasing the partitions and not for increasing
df2.show()

# COMMAND ----------

df2 = df.rdd.coalesce(4, True).toDF().withColumn("Part_id", spark_partition_id())
df2.show()

# COMMAND ----------

df2 = df.repartition(4, "DEPTNO").withColumn("Part_id", spark_partition_id())
df2.show()