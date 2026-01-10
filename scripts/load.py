# scripts/load.py - FIXED VERSION
import pandas as pd
import os
from datetime import datetime
import glob
import sys

def extract_table_name_from_filename(filename):
    """
    Extract table name from filename
    Handles cases like: sale_transactions_20260110.parquet → sale_transactions
    """
    basename = os.path.basename(filename).replace('.parquet', '')
    
    # List of known tables in your database
    known_tables = [
        'customers',
        'products', 
        'sale_transactions',
        'sale',  # Sometimes shortened
        'inventory',
        'stores',
        'sales_managers',
        'sales'  # Short for sales_managers
    ]
    
    # Check for each known table
    for table in known_tables:
        if basename.startswith(table):
            # Map shortened names to full names
            if table == 'sale':
                return 'sale_transactions'
            elif table == 'sales':
                return 'sales_managers'
            return table
    
    # Fallback: first part before underscore
    return basename.split('_')[0]

def create_gold_layer(silver_dir="/opt/airflow/data/silver", 
                     gold_dir="/opt/airflow/data/gold"):
    """
    Create business-ready aggregated tables (Gold layer)
    """
    print("="*60)
    print("🏆 CREATING GOLD LAYER (Business Aggregations)")
    print("="*60)
    
    print(f"📁 Silver directory: {silver_dir}")
    print(f"📁 Gold directory: {gold_dir}")
    
    # Create gold directory
    os.makedirs(gold_dir, exist_ok=True)
    
    try:
        # Find all Parquet files in silver
        search_pattern = os.path.join(silver_dir, "**", "*.parquet")
        parquet_files = glob.glob(search_pattern, recursive=True)
        
        print(f"\n📁 Found {len(parquet_files)} Parquet files in Silver layer")
        
        if len(parquet_files) == 0:
            print("❌ No Parquet files found in Silver layer!")
            return {}
        
        # Group files by table name
        files_by_table = {}
        for file in parquet_files:
            table_name = extract_table_name_from_filename(file)
            
            if table_name not in files_by_table:
                files_by_table[table_name] = []
            files_by_table[table_name].append(file)
        
        print(f"📋 Tables detected: {list(files_by_table.keys())}")
        
        # Load data for key tables
        data_frames = {}
        required_tables = ['customers', 'products', 'sale_transactions']
        
        for table in required_tables:
            if table in files_by_table:
                print(f"\n📥 Loading {table}...")
                try:
                    # Read all files for this table
                    dfs = []
                    for file in files_by_table[table]:
                        df = pd.read_parquet(file)
                        dfs.append(df)
                        print(f"   → {os.path.basename(file)}: {len(df)} rows")
                    
                    if dfs:
                        data_frames[table] = pd.concat(dfs, ignore_index=True)
                        print(f"   ✅ Total: {len(data_frames[table])} rows, {len(data_frames[table].columns)} columns")
                    else:
                        print(f"   ⚠️  No data loaded for {table}")
                except Exception as e:
                    print(f"   ❌ Error loading {table}: {e}")
            else:
                print(f"\n⚠️  {table} not found in Silver layer")
                # Try alternative names
                if table == 'sale_transactions':
                    if 'sale' in files_by_table:
                        print(f"   Using 'sale' instead of 'sale_transactions'")
                        table = 'sale'
                        if table in files_by_table:
                            dfs = []
                            for file in files_by_table[table]:
                                df = pd.read_parquet(file)
                                dfs.append(df)
                            data_frames['sale_transactions'] = pd.concat(dfs, ignore_index=True)
        
        # Check if we have the required tables
        if not all(table in data_frames for table in ['customers', 'products', 'sale_transactions']):
            print("\n❌ Missing required tables for Gold layer!")
            print(f"   Have: {list(data_frames.keys())}")
            print(f"   Need: ['customers', 'products', 'sale_transactions']")
            return {}
        
        # Get the dataframes
        sales = data_frames['sale_transactions']
        customers = data_frames['customers']
        products = data_frames['products']
        
        print(f"\n📊 Data ready for Gold layer:")
        print(f"   sales: {len(sales)} transactions")
        print(f"   customers: {len(customers)} customers")
        print(f"   products: {len(products)} products")
        
        # Create business aggregations
        results = {}
        
        print("\n" + "="*60)
        print("💰 CREATING BUSINESS AGGREGATIONS")
        print("="*60)
        
        # 1. DAILY SALES SUMMARY
        print("\n1. 📅 Creating Daily Sales Summary...")
        
        # Find date column
        date_columns = [col for col in sales.columns if 'date' in col.lower() or 
                       'created' in col.lower() or 'time' in col.lower()]
        
        if not date_columns:
            date_columns = list(sales.columns)
        
        date_col = date_columns[0]
        print(f"   Using column '{date_col}' for dates")
        
        # Convert to datetime and extract date
        sales['sale_date'] = pd.to_datetime(sales[date_col], errors='coerce').dt.date
        
        # Remove rows without dates
        sales_with_dates = sales.dropna(subset=['sale_date'])
        
        # Group by date
        daily_sales = sales_with_dates.groupby('sale_date').agg({
            'transaction_id': 'count',
            'quantity': 'sum'
        }).rename(columns={
            'transaction_id': 'total_orders',
            'quantity': 'total_items'
        }).reset_index()
        
        # Add revenue if we have unit_price
        if 'unit_price' in sales.columns:
            sales_with_dates['revenue'] = sales_with_dates['unit_price'] * sales_with_dates['quantity']
            daily_revenue = sales_with_dates.groupby('sale_date')['revenue'].sum().reset_index()
            daily_sales = pd.merge(daily_sales, daily_revenue, on='sale_date')
            print(f"   ✅ Added revenue calculations")
        
        # Sort by date
        daily_sales = daily_sales.sort_values('sale_date')
        
        # Save
        output_path = os.path.join(gold_dir, "daily_sales_summary.parquet")
        daily_sales.to_parquet(output_path, index=False)
        results['daily_sales_summary'] = output_path
        print(f"   ✅ Saved: {output_path}")
        print(f"   📊 {len(daily_sales)} days of sales data")
        
        # 2. TOP CUSTOMERS
        print("\n2. 👑 Creating Top Customers...")
        
        if 'customer_id' in sales.columns and 'unit_price' in sales.columns:
            # Calculate revenue per transaction
            sales['revenue'] = sales['unit_price'] * sales['quantity']
            
            # Group by customer
            customer_spend = sales.groupby('customer_id').agg({
                'transaction_id': 'count',
                'quantity': 'sum',
                'revenue': 'sum'
            }).rename(columns={
                'transaction_id': 'order_count',
                'quantity': 'total_items',
                'revenue': 'total_spent'
            }).reset_index()
            
            # Merge with customer info
            top_customers = pd.merge(customer_spend, customers, on='customer_id', how='left')
            
            # Select top 10 by spend
            top_customers = top_customers.sort_values('total_spent', ascending=False).head(10)
            
            # Save
            output_path = os.path.join(gold_dir, "top_customers.parquet")
            top_customers.to_parquet(output_path, index=False)
            results['top_customers'] = output_path
            print(f"   ✅ Saved: {output_path}")
            print(f"   💰 Top customer spent: ${top_customers['total_spent'].iloc[0]:,.2f}")
        else:
            print("   ⚠️  Skipping: Missing customer_id or unit_price in sales data")
        
        # 3. PRODUCT PERFORMANCE
        print("\n3. 📦 Creating Product Performance...")
        
        if 'product_id' in sales.columns:
            # Group by product
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
            
            # Sort by popularity
            product_performance = product_performance.sort_values('times_sold', ascending=False)
            
            # Save
            output_path = os.path.join(gold_dir, "product_performance.parquet")
            product_performance.to_parquet(output_path, index=False)
            results['product_performance'] = output_path
            print(f"   ✅ Saved: {output_path}")
            print(f"   🏆 Most popular product sold {product_performance['times_sold'].iloc[0]} times")
        else:
            print("   ⚠️  Skipping: Missing product_id in sales data")
        
        # 4. STORE PERFORMANCE (if stores data available)
        if 'stores' in data_frames and 'store_id' in sales.columns:
            print("\n4. 🏪 Creating Store Performance...")
            
            stores = data_frames['stores']
            
            # Group by store
            store_performance = sales.groupby('store_id').agg({
                'transaction_id': 'count',
                'quantity': 'sum',
                'unit_price': lambda x: (x * sales.loc[x.index, 'quantity']).sum()
            }).rename(columns={
                'transaction_id': 'total_transactions',
                'quantity': 'total_items',
                'unit_price': 'total_revenue'
            }).reset_index()
            
            # Merge with store info
            store_performance = pd.merge(store_performance, stores, on='store_id', how='left')
            
            # Save
            output_path = os.path.join(gold_dir, "store_performance.parquet")
            store_performance.to_parquet(output_path, index=False)
            results['store_performance'] = output_path
            print(f"   ✅ Saved: {output_path}")
        
        print("\n" + "="*60)
        print("🎉 GOLD LAYER CREATION COMPLETE")
        print("="*60)
        
        # Summary
        print("\n📊 GOLD LAYER SUMMARY:")
        for table_name, file_path in results.items():
            df = pd.read_parquet(file_path)
            size_kb = os.path.getsize(file_path) / 1024
            print(f"   ✅ {table_name}: {len(df)} rows, {size_kb:.1f} KB")
        
        return results
        
    except Exception as e:
        print(f"\n❌ Error creating Gold layer: {str(e)}")
        import traceback
        traceback.print_exc()
        raise e

def test_gold_layer():
    """Test function to verify Gold layer creation"""
    print("🧪 Testing Gold layer creation...")
    
    # First check what's in Silver
    print("\n🔍 Checking Silver layer...")
    silver_files = glob.glob("/opt/airflow/data/silver/**/*.parquet", recursive=True)
    print(f"Found {len(silver_files)} Parquet files:")
    
    for file in silver_files[:5]:  # Show first 5
        size = os.path.getsize(file) / 1024
        table = extract_table_name_from_filename(file)
        print(f"  {table}: {os.path.basename(file)} ({size:.1f} KB)")
    
    # Create Gold layer
    print("\n" + "="*60)
    results = create_gold_layer()
    
    if results:
        print("\n🎉 Gold layer created successfully!")
        return True
    else:
        print("\n❌ Gold layer creation failed")
        return False

if __name__ == "__main__":
    # Run test when script is executed directly
    test_gold_layer()
    
    # Show Gold files
    print("\n📁 Gold layer files:")
    gold_files = glob.glob("/opt/airflow/data/gold/*.parquet")
    if gold_files:
        for file in gold_files:
            size = os.path.getsize(file) / 1024
            df = pd.read_parquet(file)
            print(f"  {os.path.basename(file)}: {len(df)} rows, {size:.1f} KB")
            
            # Show preview
            print(f"  Preview:")
            print(df.head(3).to_string(index=False))
            print()
    else:
        print("  No Gold files created")