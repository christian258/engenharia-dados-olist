# Databricks notebook source
df_customers = spark.read.table("bronze_olist.customers")
df_items = spark.read.table("bronze_olist.items")
df_orders = spark.read.table("bronze_olist.orders")
df_products = spark.read.table("bronze_olist.products")

# COMMAND ----------

from pyspark.sql.functions import *
df_customers = df_customers.withColumn("customer_city",upper(df_customers["customer_city"])) \
                           .withColumn("customer_state",upper(df_customers["customer_state"]))
display(df_customers)

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS silver_olist")
spark.sql("DROP TABLE IF EXISTS silver_olist.customers")

# COMMAND ----------

df_customers.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_olist.customers")

# COMMAND ----------

df_orders = df_orders.withColumn("order_purchase_timestamp",col("order_purchase_timestamp").cast("timestamp").alias("order_purchase_timestamp"))
df_orders = df_orders.filter("order_status = 'delivered'")

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS silver_olist.orders")
df_orders.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_olist.orders")

# COMMAND ----------

df_items = df_items \
           .withColumn("price",col("price").cast("decimal(10,2)").alias("price")) \
           .withColumn("freight_value",col("freight_value").cast("decimal(10,2)").alias("freight_value"))

# COMMAND ----------

df_items.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_olist.items")

# COMMAND ----------

df_products = df_products.filter("product_category_name IS NOT NULL").select("product_id","product_category_name","product_weight_g")
df_products.write.mode("overwrite").saveAsTable("silver_olist.products")

# COMMAND ----------

