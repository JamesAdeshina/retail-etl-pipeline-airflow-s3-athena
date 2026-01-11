# dags/etl_retail_pipeline.py - FINAL WITH AWS
from airflow import DAG
from airflow.operators.python import PythonOperator
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
    'email_on_retry': False,
}

def extract_customers():
    return extract_table_to_csv("customers", limit=1000)

def extract_products():
    return extract_table_to_csv("products", limit=600)

def extract_transactions():
    return extract_table_to_csv("sale_transactions", limit=10000)

def transform_to_silver():
    return process_bronze_layer()

def create_gold():
    return create_gold_layer()

def run_quality_checks():
    return run_data_quality_checks()

# ======== ADD THIS AWS FUNCTION ========
def upload_to_s3():
    """Upload all layers to AWS S3"""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    
    # Get credentials from Airflow
    hook = S3Hook(aws_conn_id='aws_default')
    credentials = hook.get_credentials()
    
    # Import and run upload
    from aws_upload import upload_all_layers
    return upload_all_layers(
        access_key=credentials.access_key,
        secret_key=credentials.secret_key
    )

# ========================================

with DAG(
    'retail_etl_pipeline',
    default_args=default_args,
    description='Complete retail ETL pipeline with Medallion Architecture',
    schedule_interval='@daily',
    catchup=False,
    tags=['retail', 'etl', 'medallion', 'postgresql', 'aws']
) as dag:
    
    # Extraction tasks
    extract_customers_task = PythonOperator(
        task_id='extract_customers',
        python_callable=extract_customers
    )
    
    extract_products_task = PythonOperator(
        task_id='extract_products',
        python_callable=extract_products
    )
    
    extract_transactions_task = PythonOperator(
        task_id='extract_transactions',
        python_callable=extract_transactions
    )
    
    # Transformation task
    transform_task = PythonOperator(
        task_id='transform_to_silver',
        python_callable=transform_to_silver
    )
    
    # Gold layer task
    create_gold_task = PythonOperator(
        task_id='create_gold_layer',
        python_callable=create_gold
    )
    
    # Data quality task
    data_quality_task = PythonOperator(
        task_id='run_data_quality_checks',
        python_callable=run_quality_checks
    )
    
    # ======== ADD THIS AWS TASK ========
    upload_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3
    )
    # ====================================
    
    # Set task dependencies WITH AWS
    [extract_customers_task, extract_products_task, extract_transactions_task] >> transform_task
    transform_task >> create_gold_task
    create_gold_task >> data_quality_task
    data_quality_task >> upload_task  # AWS is the LAST step