# dags/extract_test.py - SIMPLE TEST FIRST
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2

def test_connection():
    """Simple test to verify PostgreSQL connection"""
    try:
        conn = psycopg2.connect(
            host="shopease_postgres",  # Docker container name
            port="5432",  # Internal port (not 5433)
            database="shopease_db",
            user="postgres",
            password="shopease123"
        )
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM customers")
        count = cur.fetchone()[0]
        print(f"Connected! Customers: {count}")
        conn.close()
        return True
    except Exception as e:
        print(f"Failed: {e}")
        return False

# Use LAST MONTH as start date
default_args = {
    'owner': 'Shopverse',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 9),  # LAST MONTH
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'test_postgres_connection',
    default_args=default_args,
    description='Test PostgreSQL connection from Airflow',
    schedule_interval='@once',  # Manual trigger only
    catchup=False,
    tags=['test']
) as dag:
    
    test_task = PythonOperator(
        task_id='test_postgres_connection',
        python_callable=test_connection
    )