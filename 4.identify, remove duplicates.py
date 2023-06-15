# Databricks notebook source
# MAGIC %md
# MAGIC ###### Drop Duplicates
# MAGIC - dropDuplicates() or drop_duplicates()
# MAGIC - disntict()
# MAGIC - window function row_number()
# MAGIC - groupby

# COMMAND ----------

dbutils.fs.put("/user/hive/warehouse/MyFiles/duplicates.csv","""id,name,loc,updated_date
1,ravi,bangalore,2021-01-01
1,ravi,chennai,2022-02-02
1,ravi,Hyderabad,2022-06-10
2,Raj,bangalore,2021-01-01
2,Raj,chennai,2022-02-02
3,Raj,Hyderabad,2022-06-10
4,Prasad,bangalore,2021-01-01
5,Mahesh,chennai,2022-02-02
4,Prasad,Hyderabad,2022-06-10""",True)

# COMMAND ----------

df = spark.read.option("header","true").csv('/user/hive/warehouse/MyFiles/duplicates.csv', inferSchema = True)
display(df)

# COMMAND ----------

display(df.drop_duplicates())
# duplicates not removed because the rows are actually not duplicates because
# while id, name columns have same values, loc and date do not.

# COMMAND ----------

display(df.dropDuplicates(["id"]))
# drop duplicates by default will keep the first and remove the rest

# COMMAND ----------

display(df.orderBy(df["updated_date"].desc()).dropDuplicates(["id"]))
# drop duplicates by default will keep the first and remove the rest

# COMMAND ----------

# using window function
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
df1 = df.withColumn("rownum",row_number().over(Window.partitionBy("id").orderBy(df["updated_date"].desc())))

# COMMAND ----------

display(df1.filter("rownum=1"))