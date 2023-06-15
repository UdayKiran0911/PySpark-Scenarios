# Databricks notebook source
# MAGIC %fs ls user/hive/warehouse/

# COMMAND ----------

dbutils.fs.put('user/hive/warehouse/MyFiles/skiplines.csv',"""line1
line2
line3
id,name,loc
1,Raj,Delhi
2,Ravi,Pune
3,Krishna,Bangalore
4,Sudha,Hyderabad
5,Praneeth,Chennai
""",True)

# COMMAND ----------

spark.read.csv("/user/hive/warehouse/MyFiles/skiplines.csv").show()

# COMMAND ----------

# create a rdd from spark context
rdd = sc.textFile("/user/hive/warehouse/MyFiles/skiplines.csv")
rdd.zipWithIndex().collect()

# COMMAND ----------

rdd.zipWithIndex().filter(lambda a: a[1]>2).collect()

# COMMAND ----------

rdd_final = rdd.zipWithIndex().filter(lambda a: a[1]>2).map(lambda a: a[0].split(","))

# COMMAND ----------

rdd_final.collect()

# COMMAND ----------

allcolumns = rdd_final.collect()[0]
allcolumns

# COMMAND ----------

rdd_final.toDF().show()

# COMMAND ----------

skipline = rdd_final.first()
df = rdd_final.filter(lambda a:a!=skipline).toDF(allcolumns)

# COMMAND ----------

display(df)