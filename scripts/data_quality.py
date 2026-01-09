# scripts/data_quality.py
import pandas as pd
import os
import glob

def validate_bronze_layer(bronze_dir="/opt/airflow/data/bronze"):
    """Validate Bronze layer CSV files"""
    print("🔍 Validating Bronze Layer...")
    
    csv_files = glob.glob(os.path.join(bronze_dir, "*.csv"))
    results = {}
    
    for csv_file in csv_files:
        table_name = os.path.basename(csv_file).split('_')[0]
        try:
            df = pd.read_csv(csv_file)
            
            checks = {
                'file_exists': True,
                'row_count': len(df),
                'column_count': len(df.columns),
                'has_duplicates': df.duplicated().any(),
                'null_count': df.isnull().sum().sum(),
                'file_size_kb': os.path.getsize(csv_file) / 1024
            }
            
            # Table-specific checks
            if 'customer_id' in df.columns:
                checks['customer_id_unique'] = df['customer_id'].nunique() == len(df)
            if 'product_id' in df.columns:
                checks['product_id_not_null'] = df['product_id'].notnull().all()
            
            results[table_name] = checks
            print(f"✅ {table_name}: {len(df)} rows, {len(df.columns)} cols")
            
        except Exception as e:
            results[table_name] = {'error': str(e)}
            print(f"❌ {table_name}: {str(e)}")
    
    return results

def validate_silver_layer(silver_dir="/opt/airflow/data/silver"):
    """Validate Silver layer Parquet files"""
    print("🔍 Validating Silver Layer...")
    
    parquet_files = glob.glob(os.path.join(silver_dir, "**/*.parquet"), recursive=True)
    results = {}
    
    for pq_file in parquet_files:
        table_name = os.path.basename(pq_file).split('_')[0]
        try:
            df = pd.read_parquet(pq_file)
            
            checks = {
                'file_exists': True,
                'row_count': len(df),
                'column_count': len(df.columns),
                'has_duplicates': df.duplicated().any(),
                'null_count': df.isnull().sum().sum(),
                'file_size_kb': os.path.getsize(pq_file) / 1024,
                'partitioned': 'date=' in pq_file
            }
            
            # Data type validation
            date_cols = [col for col in df.columns if 'date' in col or 'created' in col or 'updated' in col]
            for col in date_cols:
                if col in df.columns:
                    checks[f'{col}_is_datetime'] = pd.api.types.is_datetime64_any_dtype(df[col])
            
            results[table_name] = checks
            print(f"✅ {table_name}: Parquet, {len(df)} rows")
            
        except Exception as e:
            results[table_name] = {'error': str(e)}
            print(f"❌ {table_name}: {str(e)}")
    
    return results

def validate_gold_layer(gold_dir="/opt/airflow/data/gold"):
    """Validate Gold layer business tables"""
    print("🔍 Validating Gold Layer...")
    
    parquet_files = glob.glob(os.path.join(gold_dir, "*.parquet"))
    results = {}
    
    for pq_file in parquet_files:
        table_name = os.path.basename(pq_file).replace('.parquet', '')
        try:
            df = pd.read_parquet(pq_file)
            
            checks = {
                'file_exists': True,
                'row_count': len(df),
                'column_count': len(df.columns),
                'file_size_kb': os.path.getsize(pq_file) / 1024
            }
            
            # Business rule validations
            if 'daily_sales' in table_name:
                if 'total_revenue' in df.columns:
                    checks['revenue_positive'] = (df['total_revenue'] >= 0).all()
                    checks['has_daily_data'] = len(df) > 0
                    
            if 'top_customers' in table_name:
                if 'total_spent' in df.columns:
                    checks['spent_positive'] = (df['total_spent'] >= 0).all()
                    checks['top_10_customers'] = len(df) <= 10
                    
            if 'product_performance' in table_name:
                if 'times_sold' in df.columns:
                    checks['sales_positive'] = (df['times_sold'] >= 0).all()
            
            results[table_name] = checks
            print(f"✅ {table_name}: Business table, {len(df)} rows")
            
        except Exception as e:
            results[table_name] = {'error': str(e)}
            print(f"❌ {table_name}: {str(e)}")
    
    return results

def run_data_quality_checks():
    """Run all data quality checks"""
    print("="*60)
    print("🧪 RUNNING DATA QUALITY CHECKS")
    print("="*60)
    
    all_results = {
        'bronze': validate_bronze_layer(),
        'silver': validate_silver_layer(),
        'gold': validate_gold_layer()
    }
    
    # Summary report
    print("\n" + "="*60)
    print("📊 DATA QUALITY SUMMARY")
    print("="*60)
    
    total_tables = 0
    failed_tables = 0
    
    for layer, results in all_results.items():
        print(f"\n{layer.upper()} LAYER:")
        for table, checks in results.items():
            total_tables += 1
            if 'error' in checks:
                print(f"   ❌ {table}: {checks['error']}")
                failed_tables += 1
            else:
                status = "✅ PASS" if all(v for k,v in checks.items() if k not in ['file_exists', 'row_count', 'column_count', 'file_size_kb']) else "⚠️  WARN"
                print(f"   {status} {table}: {checks.get('row_count', 'N/A')} rows")
    
    print(f"\n📈 TOTAL: {total_tables} tables, {failed_tables} failed")
    
    return all_results

if __name__ == "__main__":
    run_data_quality_checks()