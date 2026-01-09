# scripts/load.py - GOLD LAYER
import pandas as pd
import os
from datetime import datetime
import glob

def create_gold_layer(silver_dir="/opt/airflow/data/silver", 
                     gold_dir="/opt/airflow/data/gold"):
    """
    Create business-ready aggregated tables (Gold layer)
    """
    print(" Creating Gold Layer (Business Aggregations)")
    
    # Create gold directory
    os.makedirs(gold_dir, exist_ok=True)
    
    try:
        # Find all Parquet files in silver
        parquet_files = glob.glob(os.path.join(silver_dir, "**/*.parquet"), recursive=True)
        print(f" Found {len(parquet_files)} Parquet files in Silver layer")
        
        # Load data (in production, you'd use Spark or Dask for large datasets)
        data_frames = {}
        
        for file in parquet_files:
            table_name = os.path.basename(file).split('_')[0]
            if table_name not in data_frames:
                print(f" Loading {table_name}...")
                # Find all partitions for this table
                pattern = os.path.join(silver_dir, table_name, "**", f"{table_name}_*.parquet")
                all_files = glob.glob(pattern, recursive=True)
                
                if all_files:
                    # Read all partitions
                    dfs = [pd.read_parquet(f) for f in all_files[:2]]  # Limit for testing
                    data_frames[table_name] = pd.concat(dfs, ignore_index=True)
                    print(f"   Loaded {len(data_frames[table_name])} rows")
        
        # Create business aggregations
        results = {}
        
        # 1. DAILY SALES SUMMARY
        if 'sale_transactions' in data_frames and 'customers' in data_frames:
            print("\n Creating Daily Sales Summary...")
            sales = data_frames['sale_transactions']
            
            # Ensure date column exists
            if 'created_at' in sales.columns:
                sales['sale_date'] = pd.to_datetime(sales['created_at']).dt.date
                
                daily_sales = sales.groupby('sale_date').agg({
                    'transaction_id': 'count',
                    'quantity': 'sum',
                    'unit_price': lambda x: (x * sales.loc[x.index, 'quantity']).sum()
                }).rename(columns={
                    'transaction_id': 'total_orders',
                    'quantity': 'total_items',
                    'unit_price': 'total_revenue'
                }).reset_index()
                
                # Save
                output_path = os.path.join(gold_dir, "daily_sales_summary.parquet")
                daily_sales.to_parquet(output_path, index=False)
                results['daily_sales'] = output_path
                print(f"    Saved: {output_path}")
        
        # 2. TOP CUSTOMERS
        if 'sale_transactions' in data_frames and 'customers' in data_frames:
            print("\n Creating Top Customers...")
            sales = data_frames['sale_transactions']
            customers = data_frames['customers']
            
            # Calculate customer spend
            customer_spend = sales.groupby('customer_id').agg({
                'transaction_id': 'count',
                'quantity': 'sum',
                'unit_price': lambda x: (x * sales.loc[x.index, 'quantity']).sum()
            }).rename(columns={
                'transaction_id': 'order_count',
                'quantity': 'total_items',
                'unit_price': 'total_spent'
            }).reset_index()
            
            # Merge with customer info
            top_customers = pd.merge(customer_spend, customers, on='customer_id', how='left')
            top_customers = top_customers.sort_values('total_spent', ascending=False).head(10)
            
            # Save
            output_path = os.path.join(gold_dir, "top_customers.parquet")
            top_customers.to_parquet(output_path, index=False)
            results['top_customers'] = output_path
            print(f"    Saved: {output_path}")
        
        # 3. PRODUCT PERFORMANCE
        if 'sale_transactions' in data_frames and 'products' in data_frames:
            print("\n Creating Product Performance...")
            sales = data_frames['sale_transactions']
            products = data_frames['products']
            
            product_performance = sales.groupby('product_id').agg({
                'transaction_id': 'count',
                'quantity': 'sum',
                'unit_price': 'mean'
            }).rename(columns={
                'transaction_id': 'times_sold',
                'quantity': 'total_quantity',
                'unit_price': 'avg_price'
            }).reset_index()
            
            # Merge with product info
            product_performance = pd.merge(product_performance, products, on='product_id', how='left')
            
            # Save
            output_path = os.path.join(gold_dir, "product_performance.parquet")
            product_performance.to_parquet(output_path, index=False)
            results['product_performance'] = output_path
            print(f"    Saved: {output_path}")
        
        print("\n" + "="*50)
        print(" GOLD LAYER CREATION COMPLETE")
        for table, path in results.items():
            print(f"   {table}: {path}")
        
        return results
        
    except Exception as e:
        print(f" Error creating Gold layer: {str(e)}")
        raise e

if __name__ == "__main__":
    print(" Testing Gold layer creation...")
    results = create_gold_layer()
    
    # List all gold files
    print("\n Gold layer files:")
    import glob
    gold_files = glob.glob("/opt/airflow/data/gold/*.parquet")
    for f in gold_files:
        size = os.path.getsize(f) / 1024  # KB
        print(f"   {os.path.basename(f)} ({size:.1f} KB)")