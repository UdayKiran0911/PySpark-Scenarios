# Databricks notebook source
# MAGIC %fs ls '/user/hive/warehouse/'

# COMMAND ----------

dbutils.fs.put('/user/hive/warehouse/MyFiles/dynamic_columns.csv',"""
id,name,loc,mail,phone
1,Ravi
2,Ram,Bangalore
3,Prasad,Chennai,sample@mail.com,1234567890
4,Sam,Pune
""",True)

# COMMAND ----------

df1 = spark.read.option("header","true").csv('/user/hive/warehouse/MyFiles/dynamic_columns.csv')
df1.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC - %fs rm -r '/user/hive/warehouse/MyFiles/dynamic_columns.csv'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC - %fs rm -r '/user/hive/warehouse/MyFiles/dynamic_nocolumnname.csv'

# COMMAND ----------

dbutils.fs.put('/user/hive/warehouse/MyFiles/dynamic_nocolumnname.csv',"""1,Ravi
2,Ram,Bangalore
3,Prasad,Chennai,sample@mail.com,1234567890
4,Sam,Pune
""",True)

# COMMAND ----------

df2 = spark.read.csv('/user/hive/warehouse/MyFiles/dynamic_nocolumnname.csv')
df2.show()

# COMMAND ----------

df3 = spark.read.text('/user/hive/warehouse/MyFiles/dynamic_nocolumnname.csv')
display(df3)

# COMMAND ----------

from pyspark.sql.functions import split
df4 = df3.withColumn("SplittedCol",split("value",","))
display(df4)

# COMMAND ----------

from pyspark.sql.functions import size
df5 = df4.withColumn("Size",size("SplittedCol"))
display(df5)

# COMMAND ----------

from pyspark.sql.functions import max
maxsize = df5.select(max("Size")).collect()
print(maxsize[0][0])

# COMMAND ----------

for i in range(maxsize[0][0]):
    df5 = df5.withColumn("Col"+str(i), df5["SplittedCol"][i])

# COMMAND ----------

display(df5)

# COMMAND ----------

final_df = df5.drop("value","SplittedCol","Size")
display(final_df)

# COMMAND ----------

from pyspark.sql.functions import split, size, max
df_temp = spark.read\
.text('/user/hive/warehouse/MyFiles/dynamic_nocolumnname.csv')

for i in range(df.select(max(size(split("value",",")))).collect()[0][0]):
    df_temp = df_temp.withColumn("col"+str(i), split(df_temp["value"],",")[i])

df_temp_final = df_temp.drop("value")
display(df_temp_final)