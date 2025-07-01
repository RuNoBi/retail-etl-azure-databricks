# Databricks notebook source
df = spark.read.table("retail.raw_online_retail")
columns_to_check = ["Invoice", "StockCode", "Quantity", "Price", "InvoiceDate", "Customer_ID"]
df_clean = df.dropna(subset=columns_to_check)
df_clean.show(5)
print(f"📊 Records after cleaning: {df_clean.count()}")

# COMMAND ----------

from pyspark.sql.functions import col, to_date

df_enriched = df_clean \
    .withColumn("TotalAmount", col("Quantity") * col("Price")) \
    .withColumn("InvoiceDateOnly", to_date("InvoiceDate"))

df_enriched.select("Invoice", "Quantity", "Price", "TotalAmount", "InvoiceDateOnly").show(5)

# COMMAND ----------

from pyspark.sql.functions import sum, count

df_customer_summary = df_enriched.groupBy("Customer_ID").agg(
    sum("TotalAmount").alias("TotalSpent"),
    count("Invoice").alias("InvoiceCount")
)

df_customer_summary.orderBy(col("TotalSpent").desc()).show(5)

# COMMAND ----------

df_customer_summary.write.mode("overwrite").saveAsTable("retail.transform_online_retail")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM retail.transform_online_retail ORDER BY TotalSpent DESC LIMIT 10