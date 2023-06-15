# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC /customer/file1.csv
# MAGIC
# MAGIC /customer/file2.csv
# MAGIC
# MAGIC /customer/file3.csv
# MAGIC
# MAGIC /customer/2022/file4.csv
# MAGIC
# MAGIC /customer/2021/file5.csv
# MAGIC
# MAGIC /customer/2023/Q1/file6.csv
# MAGIC
# MAGIC /customer/2023/Q1/file7.csv
# MAGIC

# COMMAND ----------

dbutils.fs.put("/user/hive/warehouse/MyFiles/emp/emp1.csv","""id,name,loc,updated_date
1,ravi,Hyderabad,2022-06-10
2,Raj,chennai,2022-02-02
3,Raj,Hyderabad,2022-06-10
5,Mahesh,chennai,2022-02-02
4,Prasad,Hyderabad,2022-06-10""",True)

dbutils.fs.put("/user/hive/warehouse/MyFiles/emp/emp2.csv","""id,name,loc,updated_date
1,ravi,Hyderabad,2022-06-10
2,Raj,chennai,2022-02-02
3,Raj,Hyderabad,2022-06-10
5,Mahesh,chennai,2022-02-02
4,Prasad,Hyderabad,2022-06-10""",True)

dbutils.fs.put("/user/hive/warehouse/MyFiles/emp/emp3.csv","""id,name,loc,updated_date
1,ravi,Hyderabad,2022-06-10
2,Raj,chennai,2022-02-02
3,Raj,Hyderabad,2022-06-10
5,Mahesh,chennai,2022-02-02
4,Prasad,Hyderabad,2022-06-10""",True)

dbutils.fs.put("/user/hive/warehouse/MyFiles/emp/2020/emp4.csv","""id,name,loc,updated_date
1,ravi,Hyderabad,2022-06-10
2,Raj,chennai,2022-02-02
3,Raj,Hyderabad,2022-06-10
5,Mahesh,chennai,2022-02-02
4,Prasad,Hyderabad,2022-06-10""",True)

dbutils.fs.put("/user/hive/warehouse/MyFiles/emp/2021/emp5.csv","""id,name,loc,updated_date
1,ravi,Hyderabad,2022-06-10
2,Raj,chennai,2022-02-02
3,Raj,Hyderabad,2022-06-10
5,Mahesh,chennai,2022-02-02
4,Prasad,Hyderabad,2022-06-10""",True)

dbutils.fs.put("/user/hive/warehouse/MyFiles/emp/2022/Q1/emp6.csv","""id,name,loc,updated_date
1,ravi,Hyderabad,2022-06-10
2,Raj,chennai,2022-02-02
3,Raj,Hyderabad,2022-06-10
5,Mahesh,chennai,2022-02-02
4,Prasad,Hyderabad,2022-06-10""",True)

dbutils.fs.put("/user/hive/warehouse/MyFiles/emp/2022/Q2/emp7.csv","""id,name,loc,updated_date
1,ravi,Hyderabad,2022-06-10
2,Raj,chennai,2022-02-02
3,Raj,Hyderabad,2022-06-10
5,Mahesh,chennai,2022-02-02
4,Prasad,Hyderabad,2022-06-10""",True)

# COMMAND ----------

from pyspark.sql.functions import input_file_name
df1 =  spark.read.option("recursiveFileLookup","true").option("header","true").format("csv").load("/user/hive/warehouse/MyFiles/emp/")\
.withColumn("SourceLoc",input_file_name())

# COMMAND ----------

display(df1)

# COMMAND ----------

df1.groupby("SourceLoc").count().show()