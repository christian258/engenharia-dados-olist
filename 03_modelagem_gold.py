# Databricks notebook source
dim_clientes = spark.read.table("silver_olist.customers").select("customer_id", "customer_city", "customer_state")
spark.sql("CREATE DATABASE IF NOT EXISTS gold_olist")
spark.sql("DROP TABLE IF EXISTS gold_olist.dim_clientes")
dim_clientes.write.mode("overwrite").saveAsTable("gold_olist.dim_clientes")

# COMMAND ----------

df_orders = spark.read.table("silver_olist.orders")
df_items = spark.read.table("silver_olist.items")
df_products = spark.read.table("silver_olist.products")

# COMMAND ----------

from pyspark.sql.functions import col
df_fat_vendas = df_orders.join(df_items, "order_id", "inner")
df_fat_vendas = df_fat_vendas.select(df_orders.order_id, "customer_id", "product_id", "order_purchase_timestamp", (col("price") + col("freight_value")).alias("revenue"))

# COMMAND ----------

df_fat_vendas.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_olist.fat_vendas")
df_products.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_olist.dim_produtos")

# COMMAND ----------

# Otimização da Tabela Fato para leitura rápida
print("Otimizando a Gold para o Dashboard...")

spark.sql("""
    OPTIMIZE gold_olist.fat_vendas
    ZORDER BY (customer_id, order_id)
""")

print("Tabela compactada e indexada!")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     produtos.product_category_name,
# MAGIC     SUM(vendas.revenue) as total_vendas
# MAGIC FROM 
# MAGIC     gold_olist.fat_vendas as vendas
# MAGIC JOIN 
# MAGIC     gold_olist.dim_produtos produtos ON vendas.product_id = produtos.product_id
# MAGIC GROUP BY 
# MAGIC     produtos.product_category_name
# MAGIC ORDER BY 
# MAGIC     total_vendas DESC
# MAGIC LIMIT 5;

# COMMAND ----------

