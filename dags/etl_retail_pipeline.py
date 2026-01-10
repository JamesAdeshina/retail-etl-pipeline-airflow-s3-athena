# dags/etl_retail_pipeline.py - COMPLETE VERSION
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

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
    'email_on_failure': False,
    'email': False
}

# Define all extraction functions
def extract_table(table_name, limit=None):
    """Generic extraction function"""
    return extract_table_to_csv(table_name, limit=limit)

# Create DAG
with DAG(
    'retail_etl_pipeline',
    default_args=default_args,
    description='Complete retail ETL pipeline with data quality',
    schedule_interval='@daily',
    catchup=False,
    tags=['retail', 'etl', 'data-quality', 'medallion']
) as dag:
    
    # EXTRACTION TASKS (parallel)
    extract_tasks = []
    tables_to_extract = [
        ('customers', 1000),
        ('products', 1000),
        ('sale_transactions', 5000),  # Limit due to 1M+ rows
        ('inventory', 1000),
        ('stores', None),  # Small table
        ('sales_managers', None)  # Small table
    ]
    
    for table_name, limit in tables_to_extract:
        task = PythonOperator(
            task_id=f'extract_{table_name}',
            python_callable=lambda t=table_name, l=limit: extract_table(t, l)
        )
        extract_tasks.append(task)
    
    # TRANSFORMATION TASK
    transform_task = PythonOperator(
        task_id='transform_bronze_to_silver',
        python_callable=process_bronze_layer
    )
    
    # LOAD TASK (Gold layer)
    load_task = PythonOperator(
        task_id='create_gold_aggregations',
        python_callable=create_gold_layer
    )
    
    # DATA QUALITY TASK
    data_quality_task = PythonOperator(
        task_id='data_quality_check',
        python_callable=run_data_quality_checks
    )
    # SET DEPENDENCIES
    extract_tasks >> transform_task >> load_task >> data_quality_task 