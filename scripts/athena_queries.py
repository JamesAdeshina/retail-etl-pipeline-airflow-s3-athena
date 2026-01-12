import boto3
import time

class AthenaQueries:
    def __init__(self, access_key, secret_key, region='eu-west-2'):
        """Initialize Athena client"""
        self.athena = boto3.client(
            'athena',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region
        )
        self.s3_output = 's3://shopease-athena-results-james/'  
    
    def execute_query(self, database, query):
        """Execute Athena query and wait for results"""
        print(f" Executing query on {database}...")
        
        # Start query execution
        response = self.athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': database
            },
            ResultConfiguration={
                'OutputLocation': self.s3_output
            }
        )
        
        query_id = response['QueryExecutionId']
        print(f"   Query ID: {query_id}")
        
        # Wait for completion
        while True:
            status = self.athena.get_query_execution(QueryExecutionId=query_id)
            state = status['QueryExecution']['Status']['State']
            
            if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            
            print(f"    Status: {state}")
            time.sleep(2)
        
        if state == 'SUCCEEDED':
            print(f"    Query succeeded")
            
            # Get results
            results = self.athena.get_query_results(QueryExecutionId=query_id)
            
            # Print first 10 rows
            rows = results['ResultSet']['Rows']
            print(f"\n Results (first {min(10, len(rows)-1)} rows):")
            
            # Header
            headers = [col['VarCharValue'] for col in rows[0]['Data']]
            print(' | '.join(headers))
            print('-' * 50)
            
            # Data rows
            for row in rows[1:11]:  # First 10 data rows
                values = [col.get('VarCharValue', 'NULL') for col in row['Data']]
                print(' | '.join(values))
            
            return rows
            
        else:
            print(f" Query failed: {state}")
            return None
    
    def run_business_queries(self):
        """Run sample business queries on Gold layer"""
        database = 'shopease_analytics'
        
        queries = {
            'daily_revenue': """
                SELECT 
                    sale_date,
                    total_orders,
                    total_items,
                    revenue,
                    ROUND(revenue / total_items, 2) as avg_item_value
                FROM daily_sales_summary
                WHERE revenue > 0
                ORDER BY sale_date DESC
                LIMIT 10
            """,
            
            'top_performing_customers': """
                SELECT 
                    customer_id,
                    total_spent,
                    order_count,
                    total_items,
                    ROUND(total_spent / order_count, 2) as avg_order_value,
                    city,
                    country
                FROM top_customers
                ORDER BY total_spent DESC
                LIMIT 5
            """,
            
            'product_analysis': """
                SELECT 
                    product_id,
                    product_name,
                    category,
                    times_sold,
                    total_quantity,
                    ROUND(avg_price, 2) as avg_price,
                    ROUND((times_sold * 1.0) / (SELECT MAX(times_sold) FROM product_performance), 2) as popularity_score
                FROM product_performance
                ORDER BY times_sold DESC
                LIMIT 10
            """,
            
            'revenue_by_category': """
                SELECT 
                    category,
                    SUM(times_sold) as total_sales,
                    SUM(total_quantity) as total_units,
                    ROUND(SUM(times_sold * avg_price), 2) as estimated_revenue
                FROM product_performance
                GROUP BY category
                ORDER BY estimated_revenue DESC
            """
        }
        
        print("="*60)
        print(" RUNNING ATHENA BUSINESS QUERIES")
        print("="*60)
        
        results = {}
        for query_name, query in queries.items():
            print(f"\n {query_name.replace('_', ' ').title()}...")
            result = self.execute_query(database, query)
            results[query_name] = result
        
        return results

if __name__ == "__main__":
    import os
    AWS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET = os.getenv('AWS_SECRET_ACCESS_KEY')
    
    if AWS_KEY and AWS_SECRET:
        athena = AthenaQueries(AWS_KEY, AWS_SECRET, region='eu-west-2')
        athena.run_business_queries()
    else:
        print(" Set AWS credentials as environment variables")