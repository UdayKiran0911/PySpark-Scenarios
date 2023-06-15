# Databricks notebook source
# Read file
df1 = spark.read.format("csv")\
.option("header", "true")\
.option("inferSchema", "true")\
.option("nullValue", "null")\
.load("dbfs:/FileStore/shared_uploads/udaykiran2487@gmail.com/emp.csv")

# COMMAND ----------

display(df1)

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

# convert hiredate into yyyy-MM-dd format
# and null dates
from pyspark.sql.functions import to_date

# COMMAND ----------

# to_date("<source column","existing format")
df2 = df1.withColumn("HIREDATE", to_date("HIREDATE","yyyy-MM-dd"))
df2 = df2.fillna({"HIREDATE":"9999-12-31"})
df2.show()

# COMMAND ----------

# Extracting Year, Month and Day from hire date
from pyspark.sql.functions import date_format
df3 = df2\
.withColumn("H_YEAR",date_format("HIREDATE","yyyy"))\
.withColumn("H_MONTH",date_format("HIREDATE","MM"))\
.withColumn("H_DAY",date_format("HIREDATE","dd"))

# COMMAND ----------

df3.show(3)

# COMMAND ----------

# writing as delta table with partitions
df3.write.mode("overwrite").format("delta").partitionBy("H_YEAR","H_MONTH").saveAsTable("emp_part")

# COMMAND ----------

# MAGIC %fs ls user/hive/warehouse/emp_part/

# COMMAND ----------

# MAGIC %fs ls user/hive/warehouse/emp_part/H_YEAR=1980/H_MONTH=12/

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp_part;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp_part where H_year = 1981;

# COMMAND ----------

# MAGIC %sql
# MAGIC explain select * from emp_part where H_year = 1981;

# COMMAND ----------

# MAGIC %md
# MAGIC - == Physical Plan ==
# MAGIC *(1) Project [EMPNO#2321, ENAME#2322, JOB#2323, MGR#2324, HIREDATE#2325, SAL#2326, COMM#2327, DEPTNO#2328, UPDATED_DATE#2329, H_YEAR#2330, H_MONTH#2331, H_DAY#2332]
# MAGIC +- *(1) ColumnarToRow
# MAGIC    +- FileScan parquet spark_catalog.default.emp_part[EMPNO#2321,ENAME#2322,JOB#2323,MGR#2324,HIREDATE#2325,SAL#2326,COMM#2327,DEPTNO#2328,UPDATED_DATE#2329,H_DAY#2332,H_YEAR#2330,H_MONTH#2331] 
# MAGIC - Batched: true, 
# MAGIC - DataFilters: [], 
# MAGIC - Format: Parquet, 
# MAGIC - Location: PreparedDeltaFileIndex(1 paths)[dbfs:/user/hive/warehouse/emp_part], 
# MAGIC - PartitionFilters: [isnotnull(H_YEAR#2330), (cast(H_YEAR#2330 as int) = 1981)], 
# MAGIC - PushedFilters: [], 
# MAGIC - ReadSchema: struct<EMPNO:int,ENAME:string,JOB:string,MGR:int,HIREDATE:date,SAL:int,COMM:int,DEPTNO:int,UPDATE...
# MAGIC
# MAGIC
# MAGIC _**Observe the PartitionFilters, spark is trying to cast the H_Year to int - if you check the schema of the table, you will that H_YEAR, H_MONTH,H_DAY are string types, and because we have given integer 1981 in the where condition it is trying to cast the column and then perform the operation**_
# MAGIC

# COMMAND ----------

df3.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- we can rewrite query as either
# MAGIC explain select * from emp_part where H_year = '1981';
# MAGIC -- or change the type of year, month, and day to int using cast

# COMMAND ----------

'''
== Physical Plan ==
*(1) Project [EMPNO#2458, ENAME#2459, JOB#2460, MGR#2461, HIREDATE#2462, SAL#2463, COMM#2464, DEPTNO#2465, UPDATED_DATE#2466, H_YEAR#2467, H_MONTH#2468, H_DAY#2469]
+- *(1) ColumnarToRow
   +- FileScan parquet spark_catalog.default.emp_part[EMPNO#2458,ENAME#2459,JOB#2460,MGR#2461,HIREDATE#2462,SAL#2463,COMM#2464,DEPTNO#2465,UPDATED_DATE#2466,H_DAY#2469,H_YEAR#2467,H_MONTH#2468] Batched: true, DataFilters: [], Format: Parquet, Location: PreparedDeltaFileIndex(1 paths)[dbfs:/user/hive/warehouse/emp_part], PartitionFilters: [isnotnull(H_YEAR#2467), (H_YEAR#2467 = 1981)], PushedFilters: [], ReadSchema: struct<EMPNO:int,ENAME:string,JOB:string,MGR:int,HIREDATE:date,SAL:int,COMM:int,DEPTNO:int,UPDATE...
'''