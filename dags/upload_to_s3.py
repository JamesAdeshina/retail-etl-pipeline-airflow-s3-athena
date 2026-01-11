# dags/upload_to_s3.py - SIMPLE VERSION
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

def upload_to_s3_simple():
    """Simple AWS S3 upload - manual trigger only"""
    try:
        # Try to import and use your existing aws_upload.py
        from aws_upload import upload_all_layers
        
        # Get credentials from environment (set in Airflow connection)
        import os
        aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        
        if aws_access_key and aws_secret_key:
            print(" Uploading to AWS S3...")
            results = upload_all_layers(aws_access_key, aws_secret_key)
            return results
        else:
            print(" AWS credentials not found in environment")
            print("Configure Airflow connection: aws_default")
            return False
            
    except Exception as e:
        print(f" AWS upload failed: {e}")
        print("\n To set up AWS:")
        print("1. Create AWS account")
        print("2. Create S3 buckets: shopease-bronze, shopease-silver, shopease-gold")
        print("3. Get AWS Access Key and Secret")
        print("4. Add Airflow connection: aws_default")
        return False

default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2025, 12, 9),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'upload_to_s3',
    default_args=default_args,
    description='Upload Medallion layers to AWS S3',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['aws', 's3', 'upload']
) as dag:
    
    upload_task = PythonOperator(
        task_id='upload_all_to_s3',
        python_callable=upload_to_s3_simple
    )