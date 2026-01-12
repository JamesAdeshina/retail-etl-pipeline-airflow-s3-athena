import boto3
from botocore.exceptions import ClientError
import time

class GlueRegistry:
    def __init__(self, access_key, secret_key, region='eu-west-2'):
        """Initialize AWS Glue client"""
        self.glue = boto3.client(
            'glue',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region
        )
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region
        )
        
    def create_database(self, database_name):
        """Create Glue database if it doesn't exist"""
        try:
            self.glue.create_database(
                DatabaseInput={
                    'Name': database_name,
                    'Description': 'ShopEase retail analytics database'
                }
            )
            print(f" Created Glue database: {database_name}")
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == 'AlreadyExistsException':
                print(f"  Database {database_name} already exists")
                return True
            else:
                print(f" Failed to create database: {e}")
                return False
    
    def register_s3_table(self, database_name, table_name, s3_path, 
                         partition_keys=None, file_format='parquet'):
        """Register S3 location as Glue table"""
        try:
            table_input = {
                'Name': table_name,
                'StorageDescriptor': {
                    'Location': s3_path,
                    'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat' if file_format == 'csv' else 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' if file_format == 'csv' else 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.serde2.OpenCSVSerde' if file_format == 'csv' else 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                    },
                    'Columns': self._infer_columns(s3_path, file_format)
                },
                'TableType': 'EXTERNAL_TABLE',
                'Parameters': {
                    'classification': file_format,
                    'project': 'shopease-retail',
                    'layer': 'gold' if 'gold' in s3_path else 'silver' if 'silver' in s3_path else 'bronze'
                }
            }
            
            if partition_keys:
                table_input['PartitionKeys'] = partition_keys
            
            # Check if table exists
            try:
                self.glue.get_table(DatabaseName=database_name, Name=table_name)
                print(f"  Table {table_name} exists, updating...")
                self.glue.update_table(
                    DatabaseName=database_name,
                    TableInput=table_input
                )
                print(f" Updated Glue table: {table_name}")
            except ClientError:
                # Table doesn't exist, create it
                self.glue.create_table(
                    DatabaseName=database_name,
                    TableInput=table_input
                )
                print(f" Created Glue table: {table_name}")
            
            return True
            
        except Exception as e:
            print(f" Failed to register table {table_name}: {e}")
            return False
    
    def _infer_columns(self, s3_path, file_format):
        """Infer column structure (simplified - in production use AWS Glue crawler)"""
        # For simplicity, we'll define known schemas
        schemas = {
            'daily_sales_summary': [
                {'Name': 'sale_date', 'Type': 'date'},
                {'Name': 'total_orders', 'Type': 'bigint'},
                {'Name': 'total_items', 'Type': 'bigint'},
                {'Name': 'revenue', 'Type': 'double'}
            ],
            'top_customers': [
                {'Name': 'customer_id', 'Type': 'bigint'},
                {'Name': 'order_count', 'Type': 'bigint'},
                {'Name': 'total_items', 'Type': 'bigint'},
                {'Name': 'total_spent', 'Type': 'double'},
                {'Name': 'card_number', 'Type': 'string'},
                {'Name': 'address', 'Type': 'string'},
                {'Name': 'city', 'Type': 'string'},
                {'Name': 'country', 'Type': 'string'},
                {'Name': 'postal_code', 'Type': 'string'},
                {'Name': 'created_at', 'Type': 'timestamp'},
                {'Name': 'updated_at', 'Type': 'timestamp'}
            ],
            'product_performance': [
                {'Name': 'product_id', 'Type': 'bigint'},
                {'Name': 'times_sold', 'Type': 'bigint'},
                {'Name': 'total_quantity', 'Type': 'bigint'},
                {'Name': 'avg_price', 'Type': 'double'},
                {'Name': 'sku', 'Type': 'string'},
                {'Name': 'product_name', 'Type': 'string'},
                {'Name': 'category', 'Type': 'string'},
                {'Name': 'price', 'Type': 'double'},
                {'Name': 'cost', 'Type': 'double'},
                {'Name': 'created_at', 'Type': 'timestamp'},
                {'Name': 'updated_at', 'Type': 'timestamp'}
            ]
        }
        
        # Extract table name from S3 path
        import re
        for table_name, columns in schemas.items():
            if table_name in s3_path.lower():
                return columns
        
        # Default schema for unknown tables
        return [
            {'Name': 'column1', 'Type': 'string'},
            {'Name': 'column2', 'Type': 'string'}
        ]
    
    def create_crawler(self, crawler_name, database_name, s3_path):
        """Create Glue crawler to auto-discover schema"""
        try:
            self.glue.create_crawler(
                Name=crawler_name,
                Role='arn:aws:iam::095235023129:role/GlueServiceRole-shopease', 
                DatabaseName=database_name,
                Targets={
                    'S3Targets': [{'Path': s3_path}]
                },
                SchemaChangePolicy={
                    'UpdateBehavior': 'UPDATE_IN_DATABASE',
                    'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
                },
                Configuration='{"Version": 1.0, "CrawlerOutput": {"Partitions": {"AddOrUpdateBehavior": "InheritFromTable"}}}'
            )
            print(f" Created crawler: {crawler_name}")
            return True
        except ClientError as e:
            print(f" Failed to create crawler: {e}")
            return False
    
    def run_crawler(self, crawler_name):
        """Run Glue crawler"""
        try:
            self.glue.start_crawler(Name=crawler_name)
            print(f" Started crawler: {crawler_name}")
            
            # Wait for completion
            while True:
                response = self.glue.get_crawler(Name=crawler_name)
                state = response['Crawler']['State']
                if state == 'READY':
                    print(f" Crawler {crawler_name} completed")
                    return True
                elif state == 'FAILED':
                    print(f" Crawler {crawler_name} failed")
                    return False
                print(f" Crawler status: {state}")
                time.sleep(10)
                
        except Exception as e:
            print(f" Failed to run crawler: {e}")
            return False

def register_all_tables(access_key, secret_key):
    """Register all Gold layer tables in Glue"""
    print("="*60)
    print(" REGISTERING TABLES IN AWS GLUE FOR ATHENA")
    print("="*60)
    
    registry = GlueRegistry(access_key, secret_key, region='eu-west-2')
    
    # 1. Create database
    database_name = 'shopease_analytics'
    registry.create_database(database_name)
    
    # 2. Register Gold layer tables
    gold_tables = {
        'daily_sales_summary': 's3://shopease-gold-james/daily_sales_summary.parquet',
        'top_customers': 's3://shopease-gold-james/top_customers.parquet',
        'product_performance': 's3://shopease-gold-james/product_performance.parquet'
    }
    
    results = {}
    for table_name, s3_path in gold_tables.items():
        print(f"\n Registering {table_name}...")
        success = registry.register_s3_table(
            database_name=database_name,
            table_name=table_name,
            s3_path=s3_path,
            file_format='parquet'
        )
        results[table_name] = success
    
    print("\n" + "="*60)
    print(" REGISTRATION SUMMARY")
    print("="*60)
    
    for table, success in results.items():
        status = "Done" if success else "failed"
        print(f"{status} {table}")
    
    print(f"\n Athena queries available at:")
    print(f"   Database: {database_name}")
    print("   Tables: daily_sales_summary, top_customers, product_performance")
    
    return results

if __name__ == "__main__":
    # For testing
    import os
    AWS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET = os.getenv('AWS_SECRET_ACCESS_KEY')
    
    if AWS_KEY and AWS_SECRET:
        register_all_tables(AWS_KEY, AWS_SECRET)
    else:
        print(" Set AWS credentials as environment variables")