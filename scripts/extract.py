# scripts/extract.py
import psycopg2
import pandas as pd
import os
from datetime import datetime
import sys

# Add warning filter to suppress pandas warnings
import warnings
warnings.filterwarnings('ignore')

def get_postgres_connection():
    """Get PostgreSQL connection"""
    return psycopg2.connect(
        host="shopease_postgres",
        port="5432",
        database="shopease_db",
        user="postgres",
        password="shopease123"
    )

def extract_table_to_csv(table_name, limit=None, output_dir="/opt/airflow/data/bronze"):
    """
    Extract PostgreSQL table to CSV
    
    Args:
        table_name: Name of the table (without schema)
        limit: Optional row limit for testing
        output_dir: Directory to save CSV files
    """
    
    print(f" Starting extraction for table: public.{table_name}")
    print(f" Output directory: {output_dir}")
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    conn = None
    try:
        # Connect to PostgreSQL
        conn = get_postgres_connection()
        print(" PostgreSQL connection established")
        
        # Build query with schema prefix
        query = f"SELECT * FROM public.{table_name}"
        if limit:
            query += f" LIMIT {limit}"
            print(f"  Extracting with LIMIT {limit} for testing")
        
        print(f" Executing query: {query}")
        
        # Read data - FIX: Use SQLAlchemy-style connection or specify connection
        # Option 1: Using pandas with psycopg2 connection (works but gives warning)
        df = pd.read_sql(query, conn)
        
        # Option 2: Using SQLAlchemy (better, but requires sqlalchemy package)
        # from sqlalchemy import create_engine
        # engine = create_engine('postgresql+psycopg2://postgres:shopease123@shopease_postgres:5432/shopease_db')
        # df = pd.read_sql(query, engine)
        
        print(f" Retrieved {len(df)} rows, {len(df.columns)} columns")
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{table_name}_{timestamp}.csv"
        output_path = os.path.join(output_dir, filename)
        
        # Save to CSV
        df.to_csv(output_path, index=False)
        file_size = os.path.getsize(output_path) / (1024 * 1024)  # MB
        
        print(f" Saved to: {output_path}")
        print(f" File size: {file_size:.2f} MB")
        
        # Show sample data
        print(f"\n Sample data (first 3 rows):")
        print(df.head(3).to_string(index=False))
        
        return output_path
        
    except Exception as e:
        print(f" Error extracting {table_name}: {str(e)}")
        print(f" Troubleshooting:")
        print(f"  1. Check table exists: SELECT * FROM public.{table_name} LIMIT 1")
        print(f"  2. Check connection parameters")
        raise e
        
    finally:
        if conn:
            conn.close()
            print(" Connection closed")

def extract_all_tables(tables=None, limit=None):
    """
    Extract multiple tables
    """
    if tables is None:
        tables = ["customers", "products", "sale_transactions", "inventory", "stores", "sales_managers"]
    
    results = {}
    for table in tables:
        try:
            output_path = extract_table_to_csv(table, limit=limit)
            results[table] = {
                "success": True,
                "path": output_path
            }
        except Exception as e:
            results[table] = {
                "success": False,
                "error": str(e)
            }
    
    return results

if __name__ == "__main__":
    # Test extraction with small limit
    print(" Starting test extraction...")
    results = extract_all_tables(["customers", "products"], limit=100)
    
    print("\n Extraction Summary:")
    for table, result in results.items():
        if result["success"]:
            print(f" {table}: Success - {result['path']}")
        else:
            print(f" {table}: Failed - {result['error']}")