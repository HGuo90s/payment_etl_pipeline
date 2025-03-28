import boto3
import json
from pathlib import Path

# define the function to upload the files to s3
def upload_to_s3(cred_file, file_name, bucket, object_name=None): 
    if object_name is None: 
        object_name = file_name
    credentials = read_cred(cred_file)
    s3_client = boto3.client(
        "s3", 
        aws_access_key_id=credentials['aws_access_key_id'],
        aws_secret_access_key=credentials['aws_secret_access_key']
    )
    try: 
        s3_client.upload_file(file_name, bucket, object_name)
        print(f"File '{file_name}' uploaded to S3 bucket '{bucket}' successfully.")
    except FileNotFoundError: 
        print(f"The file '{file_name}' was not found.")

# read credential file
def read_cred(cred_file): 
    # extract the json file
    file_path = Path(cred_file)
    file_type = file_path.suffix.lower()
    if file_type == '.json':
        with open(file_path, 'r') as f:
            credentials = json.load(f)
        return credentials    

# upload the files
def files_upload(cred_files, bucket, object_name=None):
    # specify the file names
    file_names = ['dim_date.parquet', 'dim_cust.parquet', 
                  'dim_geo.parquet', 'dim_prod.parquet', 
                  'dim_ostatus.parquet', 'dim_emp.parquet', 
                  'fact_orders.parquet']
    # upload all files
    for file in file_names: 
        upload_to_s3(cred_files, file, 
                     bucket)

# Example usage
if __name__ == "__main__":
    # Example credential file formats:
    # 1. JSON file (aws_credentials.json):
    # {
    #   "aws_access_key_id": "YOUR_ACCESS_KEY",
    #   "aws_secret_access_key": "YOUR_SECRET_KEY"
    # }
    cred_files = input("Name of AWS credential file: ")
    bucket = input("S3 bucket name: ")
    files_upload(cred_files, bucket)