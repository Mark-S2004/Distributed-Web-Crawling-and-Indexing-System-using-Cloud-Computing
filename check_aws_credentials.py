#!/usr/bin/env python3
"""
AWS Credentials Check Script

This script checks if your AWS credentials are properly configured
and have the necessary permissions to deploy the web crawler interface.
"""

import sys
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

def check_aws_credentials():
    """Check if AWS credentials are configured and have necessary permissions."""
    print("Checking AWS credentials...")
    
    try:
        # Try to get caller identity (basic credential check)
        sts = boto3.client('sts')
        identity = sts.get_caller_identity()
        print(f"✓ AWS credentials found for user: {identity['Arn']}")
        
        # Check EC2 permissions
        ec2 = boto3.client('ec2')
        ec2.describe_instances(MaxResults=5)
        print("✓ EC2 permissions verified")
        
        # Check security group permissions
        try:
            ec2.describe_security_groups(MaxResults=5)
            print("✓ Security group permissions verified")
        except ClientError as e:
            print(f"✗ Security group permission issue: {e}")
            return False
        
        # Check SQS permissions
        try:
            sqs = boto3.client('sqs')
            sqs.list_queues(MaxResults=5)
            print("✓ SQS permissions verified")
        except ClientError as e:
            print(f"✗ SQS permission issue: {e}")
            return False
        
        # Check DynamoDB permissions
        try:
            dynamodb = boto3.client('dynamodb')
            dynamodb.list_tables(Limit=5)
            print("✓ DynamoDB permissions verified")
        except ClientError as e:
            print(f"✗ DynamoDB permission issue: {e}")
            return False
        
        # Check S3 permissions
        try:
            s3 = boto3.client('s3')
            s3.list_buckets()
            print("✓ S3 permissions verified")
        except ClientError as e:
            print(f"✗ S3 permission issue: {e}")
            return False
        
        # Check CloudWatch permissions
        try:
            logs = boto3.client('logs')
            logs.describe_log_groups(limit=5)
            print("✓ CloudWatch Logs permissions verified")
        except ClientError as e:
            print(f"✗ CloudWatch Logs permission issue: {e}")
            return False
        
        print("\nAll AWS permissions verified successfully!")
        return True
        
    except NoCredentialsError:
        print("✗ No AWS credentials found. Please configure your AWS credentials.")
        print("Run 'aws configure' to set up your credentials.")
        return False
    except ClientError as e:
        print(f"✗ AWS credential error: {e}")
        return False

if __name__ == "__main__":
    success = check_aws_credentials()
    if not success:
        print("\nPlease fix the AWS credential issues before deploying to EC2.")
        sys.exit(1)
    else:
        print("\nYour AWS credentials are properly configured.")
        print("You can now run deploy_to_ec2.py to deploy the web crawler interface.")
