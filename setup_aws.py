import os
import json
import configparser
import logging
from pathlib import Path

def setup_aws_credentials():
    """
    Set up AWS credentials for cloud storage.
    This will create the necessary credentials file and configure environment variables.
    """
    logging.basicConfig(level=logging.INFO, 
                        format='%(asctime)s - %(levelname)s - %(message)s')
    
    print("AWS S3 Storage Configuration Setup")
    print("==================================")
    print("This script will help you set up AWS credentials for cloud storage.")
    print("You will need your AWS Access Key ID and Secret Access Key.")
    print("If you don't have these, you can create them in the AWS IAM console.")
    print()
    
    # Get AWS credentials from user
    access_key = input("Enter your AWS Access Key ID: ").strip()
    secret_key = input("Enter your AWS Secret Access Key: ").strip()
    region = input("Enter your preferred AWS region (default: us-east-1): ").strip() or "us-east-1"
    bucket_name = input("Enter your S3 bucket name (default: web-crawler-data-storage): ").strip() or "web-crawler-data-storage"
    
    # Create AWS credentials directory if it doesn't exist
    aws_dir = Path.home() / ".aws"
    aws_dir.mkdir(exist_ok=True)
    
    # Create credentials file
    credentials_path = aws_dir / "credentials"
    config = configparser.ConfigParser()
    
    # Read existing config if it exists
    if credentials_path.exists():
        config.read(credentials_path)
    
    # Add/update default profile
    if "default" not in config:
        config["default"] = {}
    
    config["default"]["aws_access_key_id"] = access_key
    config["default"]["aws_secret_access_key"] = secret_key
    config["default"]["region"] = region
    
    # Write credentials file
    with open(credentials_path, "w") as f:
        config.write(f)
    
    # Set environment variables for the current session
    os.environ["AWS_ACCESS_KEY_ID"] = access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = secret_key
    os.environ["AWS_DEFAULT_REGION"] = region
    os.environ["AWS_S3_BUCKET"] = bucket_name
    
    # Create a local config file for the web crawler
    config = {
        "aws": {
            "region": region,
            "bucket_name": bucket_name
        }
    }
    
    with open("aws_config.json", "w") as f:
        json.dump(config, f, indent=4)
    
    logging.info(f"AWS credentials configured successfully")
    logging.info(f"AWS credentials file created at: {credentials_path}")
    logging.info(f"Local configuration file created at: aws_config.json")
    logging.info(f"S3 bucket name set to: {bucket_name}")
    
    print()
    print("Configuration complete!")
    print(f"AWS credentials have been saved to: {credentials_path}")
    print(f"Local configuration has been saved to: aws_config.json")
    print()
    print("You can now run the web crawler with cloud storage enabled.")
    print("The crawler will store raw HTML and processed text in your S3 bucket.")

if __name__ == "__main__":
    setup_aws_credentials() 