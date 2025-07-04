# retail-etl-azure-databricks
# Online Retail ETL Project (Databricks + Delta Lake)

## Project Objective
Build a mini ETL pipeline on Databricks to clean, transform, and aggregate retail sales data and store it in Delta Lake for further analytics.

## Dataset
- Source: [Online Retail II - UCI](https://www.kaggle.com/datasets/mashlyn/online-retail-ii-uci)
- Columns: Invoice, StockCode, Description, Quantity, InvoiceDate, Price, Customer ID, Country

## Workflow Steps
1. **Extract**: Load CSV using PySpark
2. **Transform**: Clean nulls, create `TotalAmount`, extract date
3. **Aggregate**: Group by Customer_ID → calculate TotalSpent & invoiceCount
4. **Load**: Save as Delta Table in Databricks: `retail.retail_customer_summary`

## Tools & Tech
  Databricks,
  Apache Spark (PySpark),
  SQL,
  Delta Lake,
  GitHub
  
---
Project by Puttaradol Pongpanich (CPAXTRA Intern) | July 2025  
Mentored by Ankit Kushwaha | For learning purpose only
