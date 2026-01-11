# scripts/aws_upload.py - SIMPLE VERSION
import boto3
import os
import glob

def upload_all_layers(access_key, secret_key, region='eu-west-2'):
    """
    Upload all Medallion layers to AWS S3
    Returns: Dictionary with upload results
    """
    print("="*60)
    print(" UPLOADING TO AWS S3")
    print("="*60)
    
    # Initialize S3 client
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region
        )
        print(" AWS S3 client initialized")
    except Exception as e:
        print(f" Failed to initialize S3 client: {e}")
        return {"error": str(e)}
    
    # Your bucket names - CHANGE THESE TO YOUR ACTUAL BUCKET NAMES!
    buckets = {
        'bronze': 'shopease-bronze-james',    
        'silver': 'shopease-silver-james',      
        'gold': 'shopease-gold-james'         
    }
    
    results = {}
    
    for layer, bucket_name in buckets.items():
        print(f"\n Processing {layer.upper()} layer...")
        
        local_dir = f'/opt/airflow/data/{layer}'
        
        # Check if directory exists and has files
        if not os.path.exists(local_dir):
            print(f"    Directory doesn't exist: {local_dir}")
            results[layer] = {"status": "skipped", "reason": "directory not found"}
            continue
        
        files = []
        if layer == 'silver':
            # Silver has nested parquet files
            files = glob.glob(f'{local_dir}/**/*.parquet', recursive=True)
        else:
            # Bronze and Gold have flat structure
            files = glob.glob(f'{local_dir}/*')
        
        # Filter only files (not directories)
        files = [f for f in files if os.path.isfile(f)]
        
        if not files:
            print(f"    No files found in {local_dir}")
            results[layer] = {"status": "skipped", "reason": "no files"}
            continue
        
        print(f"   Found {len(files)} files")
        
        uploaded_count = 0
        for file_path in files:
            try:
                # Create S3 key (preserve directory structure)
                relative_path = os.path.relpath(file_path, '/opt/airflow/data')
                s3_key = relative_path.replace('\\', '/')
                
                # Upload file
                s3.upload_file(file_path, bucket_name, s3_key)
                print(f"    {os.path.basename(file_path)} → s3://{bucket_name}/{s3_key}")
                uploaded_count += 1
                
            except Exception as e:
                print(f"    Failed to upload {os.path.basename(file_path)}: {e}")
        
        results[layer] = {
            "status": "success" if uploaded_count > 0 else "failed",
            "uploaded": uploaded_count,
            "total": len(files),
            "bucket": bucket_name
        }
    
    print("\n" + "="*60)
    print(" UPLOAD SUMMARY")
    print("="*60)
    
    for layer, result in results.items():
        if "status" in result:
            if result["status"] == "success":
                print(f" {layer.upper()}: {result['uploaded']}/{result['total']} files → s3://{result['bucket']}")
            else:
                print(f"  {layer.upper()}: {result.get('reason', 'failed')}")
    
    return results

if __name__ == "__main__":
    # For testing from command line
    import sys
    
    if len(sys.argv) != 3:
        print("Usage: python aws_upload.py <ACCESS_KEY> <SECRET_KEY>")
        print("Or set environment variables: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY")
        sys.exit(1)
    
    access_key = sys.argv[1]
    secret_key = sys.argv[2]
    
    upload_all_layers(access_key, secret_key)