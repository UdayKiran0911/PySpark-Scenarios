# Databricks notebook source
dbutils.fs.put("/user/hive/warehouse/MyFiles/double_delimiter.csv","""id||name||loc||updated_date
1||ravi||bangalore||2021-01-01
1||ravi||chennai||2022-02-02
1||ravi||Hyderabad||2022-06-10
2||Raj||bangalore||2021-01-01
2||Raj||chennai||2022-02-02
3||Raj||Hyderabad||2022-06-10
4||Prasad||bangalore||2021-01-01
5||Mahesh||chennai||2022-02-02
4||Prasad||Hyderabad||2022-06-10""",True)

# COMMAND ----------

df = spark.read.option("header","true").csv("/user/hive/warehouse/MyFiles/double_delimiter.csv", sep="||")
display(df)

# COMMAND ----------

dbutils.fs.put("/user/hive/warehouse/MyFiles/multi_dilimiter.csv","""id,name,loc,updated_date
1,ravi,bangalore,2021-01-01
1,ravi|chennai|2022-02-02
1,ravi|Hyderabad|2022-06-10
2,Raj,bangalore|2021-01-01
2,Raj,chennai,2022-02-02
3,Raj,Hyderabad,2022-06-10
4,Prasad|bangalore|2021-01-01
5,Mahesh|chennai,2022-02-02
4,Prasad,Hyderabad,2022-06-10""",True)

# COMMAND ----------

df = spark.read.option("header","true").csv("/user/hive/warehouse/MyFiles/multi_dilimiter.csv", sep="[,,|]")
display(df)