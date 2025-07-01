# Databricks notebook source
df = spark.read.table("retail.online_retail_ii")
df.show(5)
print(f"📊 Records Before cleaning: {df.count()}")

# COMMAND ----------

df_clean = df.withColumnRenamed("Customer ID", "Customer_ID")
df_clean.write.mode("overwrite").saveAsTable("retail.raw_online_retail")