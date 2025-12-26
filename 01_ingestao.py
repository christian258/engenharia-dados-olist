# Databricks notebook source
import pandas as pd
from pyspark.sql.functions import current_timestamp

# 0. Preparar o Banco de Dados
spark.sql("CREATE DATABASE IF NOT EXISTS bronze_olist")

# URLs oficiais dos arquivos CSV
urls = {
    "customers": "https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets/olist_customers_dataset.csv",
    "orders": "https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets/olist_orders_dataset.csv",
    "items": "https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets/olist_order_items_dataset.csv",
    "products": "https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets/olist_products_dataset.csv"
}

# Função para processar cada tabela
def ingerir_tabela(nome_tabela, url):
    print(f"Baixando {nome_tabela.upper()}...")
    
    # 1. Leitura com Pandas (Lê da URL direto para a RAM)
    pdf = pd.read_csv(url)
    
    # 2. Conversão para Spark DataFrame
    df_spark = spark.createDataFrame(pdf)
    
    # 3. Adicionar Metadados (Data Carga)
    df_final = df_spark.withColumn("data_carga", current_timestamp())
    
    # 4. Salvar como Tabela Delta Gerenciada (Hive Metastore)
    caminho_tabela = f"bronze_olist.{nome_tabela}"
    df_final.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(caminho_tabela)
        
    print(f"Tabela '{caminho_tabela}' criada com {df_final.count()} registros.")

# --- Executando o Pipeline ---

# 1. Clientes
ingerir_tabela("customers", urls["customers"])

# 2. Pedidos
ingerir_tabela("orders", urls["orders"])

# 3. Itens
ingerir_tabela("items", urls["items"])

# 4. Produtos
ingerir_tabela("products", urls["products"])

print("\nSucesso!")
display(spark.sql("SHOW TABLES IN bronze_olist"))