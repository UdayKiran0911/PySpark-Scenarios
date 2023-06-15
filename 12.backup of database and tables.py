# Databricks notebook source
# MAGIC %sql
# MAGIC show databases;

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in default

# COMMAND ----------

# MAGIC %fs rm -r "/user/hive/warehouse/emp"

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists emp (id int, name string) using delta;
# MAGIC insert into emp
# MAGIC select 1, "Uday" union all
# MAGIC select 2, "Kiran" union all
# MAGIC select 3, "Bodala";
# MAGIC
# MAGIC create table if not exists prodcts (id int, name string) using delta;
# MAGIC insert into prodcts
# MAGIC select 1, "Shampoo" union all
# MAGIC select 2, "Soap" union all
# MAGIC select 3, "Dishsoap";

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.emp

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.prodcts

# COMMAND ----------

print([db.databaseName for db in spark.sql("show databases").collect()])
print([tb.name for tb in spark.catalog.listTables("default")])

# COMMAND ----------

dbutils.fs.mkdirs("file:/temp/ddls/")

# COMMAND ----------

# MAGIC %fs mkdirs file:/temp/ddls/

# COMMAND ----------

# MAGIC %fs ls file:/temp/ddls/

# COMMAND ----------

def createBackup(dbname):
    f = open("file:/tmp/ddls/bckp_{}.sql".format(dbname),"w")
    for tb in spark.catalog.listTables(dbname):
        f.write(spark.sql("show create table {}.{}".format(dbname.tbname)))
        f.write(";\n")
    f.close()

# COMMAND ----------

createBackup("default")