# Databricks notebook source
df = spark.read.table("retail.transform_online_retail")
df.write.format("delta").mode("overwrite").saveAsTable("retail.retail_customer_summary")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM retail.retail_customer_summary WHERE TotalSpent >= 50000 ORDER BY TotalSpent DESC ;