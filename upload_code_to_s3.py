#!/usr/bin/env python3
import boto3
import json
import os
import logging

def upload_code_to_s3():
    """Upload code files to S3 for instance deployment"""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Load configuration
    with open('aws_config.json', 'r') as f:
        config = json.load(f)
    
    region = config['aws']['region']
    bucket_name = config['aws']['bucket_name']
    
    # Initialize S3 client
    s3 = boto3.client('s3', region_name=region)
    
    # List of files to upload
    files_to_upload = [
        'main.py',
        'masterNode.py',
        'crawlerNode.py',
        'indexerNode.py',
        'requirements.txt',
        'cloud_storage.py',
        'db_manager.py'
    ]
    
    # Upload each file to the code directory in S3
    for filename in files_to_upload:
        if os.path.exists(filename):
            try:
                logging.info(f"Uploading {filename} to S3...")
                s3.upload_file(
                    filename, 
                    bucket_name, 
                    f"code/{filename}"
                )
                # Get the public URL
                url = f"https://{bucket_name}.s3.amazonaws.com/code/{filename}"
                logging.info(f"Uploaded {filename} to {url}")
            except Exception as e:
                logging.error(f"Error uploading {filename}: {str(e)}")
        else:
            logging.warning(f"File {filename} not found, skipping")
    
    # Create a modified bootstrap script in S3 that pulls code from S3
    bootstrap_script = f"""#!/bin/bash
# Script to download code from S3 bucket

# Create working directory
mkdir -p /home/ubuntu/crawler
cd /home/ubuntu/crawler

# Download code files from S3
FILES=(
    "main.py"
    "masterNode.py"
    "crawlerNode.py"
    "indexerNode.py"
    "requirements.txt"
    "cloud_storage.py"
    "db_manager.py"
)

for file in "${{FILES[@]}}"; do
    echo "Downloading $file from S3..."
    wget "https://{bucket_name}.s3.amazonaws.com/code/$file"
done

# Install dependencies
pip3 install -r requirements.txt

# Create logs directory
mkdir -p logs

echo "Bootstrap complete!"
"""
    
    # Upload bootstrap script to S3
    try:
        logging.info("Creating bootstrap script in S3...")
        s3.put_object(
            Bucket=bucket_name,
            Key="code/bootstrap.sh",
            Body=bootstrap_script,
            ContentType='text/plain'
        )
        bootstrap_url = f"https://{bucket_name}.s3.amazonaws.com/code/bootstrap.sh"
        logging.info(f"Bootstrap script available at: {bootstrap_url}")
        print("\nBootstrap script URL:")
        print(bootstrap_url)
        print("\nUse this URL in your instance user data to download the code.")
    except Exception as e:
        logging.error(f"Error creating bootstrap script: {str(e)}")
    
    # Try to set bucket policy for public access (if allowed by your permissions)
    try:
        logging.info("Setting bucket policy for public read access...")
        bucket_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "PublicReadForGetBucketObjects",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": f"arn:aws:s3:::{bucket_name}/code/*"
                }
            ]
        }
        
        s3.put_bucket_policy(
            Bucket=bucket_name,
            Policy=json.dumps(bucket_policy)
        )
        logging.info("Successfully set bucket policy for public access")
    except Exception as e:
        logging.warning(f"Could not set bucket policy: {str(e)}")
        logging.warning("You may need to manually set your bucket to allow public access in the AWS Console")
        print("\nWARNING: Could not set bucket policy for public access.")
        print("You may need to manually make your files public in the AWS Console:")
        print("1. Go to S3 Console")
        print("2. Open your bucket properties")
        print("3. Under 'Permissions' tab, modify the bucket policy or block public access settings")
    
    logging.info("Upload complete!")

if __name__ == "__main__":
    upload_code_to_s3() 