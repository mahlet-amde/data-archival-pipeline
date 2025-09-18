import boto3
        
def upload_file_to_s3(bucket_name, s3_key, data):
    """Uploads a local file to a specified S3 bucket."""
    s3_client = boto3.client("s3")
    try:
        # s3_client.upload_file(file_path, bucket_name, s3_key)
        s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=data)
        print(f"Successfully uploaded {s3_key} to S3 bucket {bucket_name}.")
        return True
    except Exception as e:
        print(f"Error uploading file to S3: {e}")
        return False