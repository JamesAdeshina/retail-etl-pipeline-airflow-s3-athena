# scripts/transform.py
import pandas as pd
import os
from datetime import datetime
import glob
import pyarrow as pa
import pyarrow.parquet as pq

def csv_to_parquet(input_csv, output_dir="/opt/airflow/data/silver"):
    """
    Convert CSV file to Parquet format
    
    Args:
        input_csv: Path to input CSV file
        output_dir: Directory to save Parquet files
    """
    print(f" Transforming: {input_csv}")
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        # Read CSV
        print(f" Reading CSV file...")
        df = pd.read_csv(input_csv)
        print(f" Read {len(df)} rows, {len(df.columns)} columns")
        
        # Basic data cleaning
        print("🧹 Performing basic data cleaning...")
        
        # Convert date columns
        date_columns = ['created_at', 'updated_at', 'order_date', 'signup_date', 'last_restocked']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                print(f"   Converted {col} to datetime")
        
        # Handle missing values
        numeric_cols = df.select_dtypes(include=['number']).columns
        for col in numeric_cols:
            if df[col].isnull().any():
                df[col] = df[col].fillna(0)
                print(f"   Filled NaN in {col} with 0")
        
        # Generate output filename
        filename = os.path.basename(input_csv).replace('.csv', '.parquet')
        table_name = filename.split('_')[0]
        date_part = datetime.now().strftime("%Y%m%d")
        
        # Create partitioned output path (by date)
        output_path = os.path.join(output_dir, table_name, f"date={date_part}", filename)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Save as Parquet
        print(f" Saving as Parquet to: {output_path}")
        df.to_parquet(output_path, index=False, compression='snappy')
        
        # Verify file
        file_size = os.path.getsize(output_path) / (1024 * 1024)  # MB
        print(f" Saved! File size: {file_size:.2f} MB")
        
        # Read back to verify
        df_check = pd.read_parquet(output_path)
        print(f" Verification: {len(df_check)} rows read back")
        
        return output_path
        
    except Exception as e:
        print(f" Transformation failed: {str(e)}")
        raise e

def process_bronze_layer(bronze_dir="/opt/airflow/data/bronze", 
                        silver_dir="/opt/airflow/data/silver"):
    """
    Process all CSV files in Bronze layer
    """
    print(f" Processing Bronze layer: {bronze_dir}")
    
    # Find all CSV files
    csv_files = glob.glob(os.path.join(bronze_dir, "*.csv"))
    print(f" Found {len(csv_files)} CSV files")
    
    results = {}
    for csv_file in csv_files:
        try:
            output_path = csv_to_parquet(csv_file, silver_dir)
            results[os.path.basename(csv_file)] = {
                "success": True,
                "parquet_path": output_path,
                "csv_size": os.path.getsize(csv_file) / 1024,  # KB
                "parquet_size": os.path.getsize(output_path) / 1024  # KB
            }
        except Exception as e:
            results[os.path.basename(csv_file)] = {
                "success": False,
                "error": str(e)
            }
    
    return results

if __name__ == "__main__":
    print(" Testing transformation...")
    results = process_bronze_layer()
    
    print("\n Transformation Summary:")
    for file, result in results.items():
        if result["success"]:
            print(f" {file}:")
            print(f"   CSV: {result['csv_size']:.1f} KB → Parquet: {result['parquet_size']:.1f} KB")
            print(f"   Path: {result['parquet_path']}")
        else:
            print(f"❌ {file}: {result['error']}")