# dags/etl_retail_pipeline.py - COMPLETE VERSION
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

# Add this import at the top
from extract import extract_table_to_csv
from transform import process_bronze_layer
from load import create_gold_layer
from data_quality import run_data_quality_checks

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 9),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create a function for each table
def extract_customers():
    return extract_table_to_csv("customers", limit=1000)

def extract_products():
    return extract_table_to_csv("products", limit=1000)

def extract_sale_transactions():
    return extract_table_to_csv("sale_transactions", limit=5000)  # 1M+ rows, use limit!

def extract_inventory():
    return extract_table_to_csv("inventory", limit=1000)

def extract_stores():
    return extract_table_to_csv("stores", limit=None)  # Only 5 rows

def extract_sales_managers():
    return extract_table_to_csv("sales_managers", limit=None)  # Only 5 rows

def transform_bronze_to_silver():
    from transform import process_bronze_layer
    return process_bronze_layer()

with DAG(
    'retail_etl_pipeline',
    default_args=default_args,
    description='Complete ETL pipeline for retail data',
    schedule_interval='@daily',
    catchup=False,
    tags=['retail', 'etl', 'complete']
) as dag:
    
    # Extraction tasks for ALL tables
    extract_customers_task = PythonOperator(
        task_id='extract_customers',
        python_callable=extract_customers
    )
    
    extract_products_task = PythonOperator(
        task_id='extract_products',
        python_callable=extract_products
    )
    
    extract_transactions_task = PythonOperator(
        task_id='extract_sale_transactions',
        python_callable=extract_sale_transactions
    )
    
    extract_inventory_task = PythonOperator(
        task_id='extract_inventory',
        python_callable=extract_inventory
    )
    
    extract_stores_task = PythonOperator(
        task_id='extract_stores',
        python_callable=extract_stores
    )
    
    extract_managers_task = PythonOperator(
        task_id='extract_sales_managers',
        python_callable=extract_sales_managers
    )
    
    # Transformation task
    transform_task = PythonOperator(
        task_id='transform_bronze_to_silver',
        python_callable=transform_bronze_to_silver
    )
    
    # Dependencies: ALL extracts → Transform
    [
        extract_customers_task,
        extract_products_task, 
        extract_transactions_task,
        extract_inventory_task,
        extract_stores_task,
        extract_managers_task
    ] >> transform_task

    # Add this function
    def create_gold_aggregations():
        from load import create_gold_layer
        return create_gold_layer()

    # Add this task to the DAG
    create_gold_task = PythonOperator(
        task_id='create_gold_aggregations',
        python_callable=create_gold_aggregations
    )

    # Update dependencies
    transform_task >> create_gold_task


    # Add this function
    def data_quality_check():
        """Run data quality checks on all layers"""
        return run_data_quality_checks()

    # Add this task to the DAG
    data_quality_task = PythonOperator(
        task_id='data_quality_check',
        python_callable=data_quality_check
    )

    # Update dependencies: Gold → Data Quality
    create_gold_task >> data_quality_task