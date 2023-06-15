# Databricks notebook source
dbutils.fs.put("/user/hive/warehouse/MyFiles/multiline_json.json","""[
	{
		"id": "0001",
		"type": "donut",
		"name": "Cake",
		"ppu": 0.55,
		"batters":
			{
				"batter":
					[
						{ "id": "1001", "type": "Regular" },
						{ "id": "1002", "type": "Chocolate" },
						{ "id": "1003", "type": "Blueberry" },
						{ "id": "1004", "type": "Devil's Food" }
					]
			},
		"topping":
			[
				{ "id": "5001", "type": "None" },
				{ "id": "5002", "type": "Glazed" },
				{ "id": "5005", "type": "Sugar" },
				{ "id": "5007", "type": "Powdered Sugar" },
				{ "id": "5006", "type": "Chocolate with Sprinkles" },
				{ "id": "5003", "type": "Chocolate" },
				{ "id": "5004", "type": "Maple" }
			]
	},
	{
		"id": "0002",
		"type": "donut",
		"name": "Raised",
		"ppu": 0.55,
		"batters":
			{
				"batter":
					[
						{ "id": "1001", "type": "Regular" }
					]
			},
		"topping":
			[
				{ "id": "5001", "type": "None" },
				{ "id": "5002", "type": "Glazed" },
				{ "id": "5005", "type": "Sugar" },
				{ "id": "5003", "type": "Chocolate" },
				{ "id": "5004", "type": "Maple" }
			]
	},
	{
		"id": "0003",
		"type": "donut",
		"name": "Old Fashioned",
		"ppu": 0.55,
		"batters":
			{
				"batter":
					[
						{ "id": "1001", "type": "Regular" },
						{ "id": "1002", "type": "Chocolate" }
					]
			},
		"topping":
			[
				{ "id": "5001", "type": "None" },
				{ "id": "5002", "type": "Glazed" },
				{ "id": "5003", "type": "Chocolate" },
				{ "id": "5004", "type": "Maple" }
			]
	}]""",True)

# COMMAND ----------

df = spark.read.json("/user/hive/warehouse/MyFiles/multiline_json.json", multiLine=True)
display(df)

# COMMAND ----------

# for struct type we can extract data using batters.batter
# for array type we need to perform explode operation

# COMMAND ----------

from pyspark.sql.functions import explode, col

# COMMAND ----------

# for struct data type, we indvidually extract the columns,
# for array data type we will use explode function and then extract the required columns

display(df.withColumn("topping_explode", explode(df["topping"]))\
       .withColumn("topping_id", col("topping_explode.id"))\
       .withColumn("topping_type", col("topping_explode.type"))\
        .withColumn("batters_explode", explode(col("batters.batter")))\
        .withColumn("batter_id", col("batters_explode.id"))\
        .withColumn("batter_type", col("batters_explode.type"))\
        .drop("batters","batters_explode","topping","topping_explode"))