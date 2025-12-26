# Databricks notebook source
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime

# Definição padrão da DAG
default_args = {
    'owner': 'travassoschristian@gmail.com',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    'pipeline_olist_ecommerce', # Nome da DAG
    default_args=default_args,
    description='Pipeline Bronze -> Silver -> Gold no Databricks',
    schedule_interval='07 0 * * *', 
    start_date=datetime(2025, 12, 19),
    catchup=False
) as dag:

    # Tarefa 1: Ingestão
    task_bronze = DatabricksSubmitRunOperator(
        task_id='ingestao_bronze',
        databricks_conn_id='databricks_default', 
        existing_cluster_id='857531dc4e465070', 
        notebook_task={
            'notebook_path': '/Users/travassoschristian@gmail.com/Engenharia_olist_v2/01_ingestao'
        }
    )

    # Tarefa 2: Silver (Depende da Bronze)
    task_silver = DatabricksSubmitRunOperator(
        task_id='tratamento_silver',
        databricks_conn_id='databricks_default',
        existing_cluster_id='857531dc4e465070',
        notebook_task={
            'notebook_path': '/Users/travassoschristian@gmail.com/Engenharia_olist_v2/02_tratamento_silver'
        }
    )

    # Tarefa 3: Gold (Depende da Silver)
    task_gold = DatabricksSubmitRunOperator(
        task_id='modelagem_gold',        
        databricks_conn_id='databricks_default',
        existing_cluster_id='857531dc4e465070',
        notebook_task={
            'notebook_path': '/Users/travassoschristian@gmail.com/Engenharia_olist_v2/03_modelagem_gold'
        }
    )

    # --- DEFINIÇÃO DE DEPENDÊNCIAS ---
    # Aqui é onde a mágica acontece. Use '>>' para encadear.
    # Exemplo: task_1 >> task_2
    
    task_bronze >> task_silver >> task_gold