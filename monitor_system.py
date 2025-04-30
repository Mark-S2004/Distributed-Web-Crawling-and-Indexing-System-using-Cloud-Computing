#!/usr/bin/env python3
import boto3
import argparse
import time
import logging
import json
import os
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def get_sqs_queue_stats(queue_name):
    """Get statistics about an SQS queue"""
    sqs = boto3.client('sqs')
    
    try:
        # Get queue URL
        response = sqs.get_queue_url(QueueName=queue_name)
        queue_url = response['QueueUrl']
        
        # Get queue attributes
        response = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=[
                'ApproximateNumberOfMessages',
                'ApproximateNumberOfMessagesNotVisible',
                'ApproximateNumberOfMessagesDelayed'
            ]
        )
        
        return {
            'QueueName': queue_name,
            'QueueUrl': queue_url,
            'ApproximateNumberOfMessages': int(response['Attributes']['ApproximateNumberOfMessages']),
            'ApproximateNumberOfMessagesNotVisible': int(response['Attributes']['ApproximateNumberOfMessagesNotVisible']),
            'ApproximateNumberOfMessagesDelayed': int(response['Attributes']['ApproximateNumberOfMessagesDelayed']),
            'TotalMessages': int(response['Attributes']['ApproximateNumberOfMessages']) + 
                          int(response['Attributes']['ApproximateNumberOfMessagesNotVisible']) + 
                          int(response['Attributes']['ApproximateNumberOfMessagesDelayed'])
        }
    except Exception as e:
        logging.error(f"Error getting SQS queue stats for {queue_name}: {e}")
        return None

def get_ec2_instance_info(stack_name=None):
    """Get information about EC2 instances"""
    ec2 = boto3.client('ec2')
    
    try:
        # Set up filters
        filters = []
        if stack_name:
            filters.append({
                'Name': 'tag:aws:cloudformation:stack-name',
                'Values': [stack_name]
            })
        
        # Get instance information
        response = ec2.describe_instances(Filters=filters)
        
        instances = []
        for reservation in response['Reservations']:
            for instance in reservation['Instances']:
                # Get instance name from tags
                instance_name = 'Unknown'
                for tag in instance.get('Tags', []):
                    if tag['Key'] == 'Name':
                        instance_name = tag['Value']
                        break
                
                instances.append({
                    'InstanceId': instance['InstanceId'],
                    'InstanceType': instance['InstanceType'],
                    'State': instance['State']['Name'],
                    'PublicIpAddress': instance.get('PublicIpAddress', 'None'),
                    'PublicDnsName': instance.get('PublicDnsName', 'None'),
                    'LaunchTime': instance['LaunchTime'].strftime('%Y-%m-%d %H:%M:%S'),
                    'Name': instance_name
                })
        
        return instances
    
    except Exception as e:
        logging.error(f"Error getting EC2 instance info: {e}")
        return []

def check_s3_bucket(bucket_name):
    """Check S3 bucket for data"""
    s3 = boto3.client('s3')
    
    try:
        # Check if bucket exists
        s3.head_bucket(Bucket=bucket_name)
        
        # List objects in the bucket with a prefix
        response = s3.list_objects_v2(
            Bucket=bucket_name,
            MaxKeys=10
        )
        
        return {
            'BucketName': bucket_name,
            'ObjectCount': response.get('KeyCount', 0),
            'SampleObjects': [obj['Key'] for obj in response.get('Contents', [])]
        }
    
    except Exception as e:
        logging.error(f"Error checking S3 bucket {bucket_name}: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description='Monitor the distributed crawler system')
    parser.add_argument('--sqs-queue', default='crawler-url-queue',
                      help='Name of the SQS queue for task distribution')
    parser.add_argument('--status-queue', default='crawler-status-queue',
                      help='Name of the SQS queue for status updates')
    parser.add_argument('--s3-bucket', default='web-crawler-data-storage',
                      help='Name of the S3 bucket for crawler data')
    parser.add_argument('--stack-name', default='web-crawler-single-instance',
                      help='Name of the CloudFormation stack')
    parser.add_argument('--continuous', action='store_true',
                      help='Run in continuous monitoring mode')
    parser.add_argument('--interval', type=int, default=30,
                      help='Interval in seconds between updates in continuous mode')
    
    args = parser.parse_args()
    
    def display_status():
        # Clear screen on Windows or Unix
        os.system('cls' if os.name == 'nt' else 'clear')
        
        print(f"\n{'='*80}")
        print(f"DISTRIBUTED WEB CRAWLER SYSTEM MONITOR - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*80}\n")
        
        # Get SQS queue statistics
        task_queue_stats = get_sqs_queue_stats(args.sqs_queue)
        status_queue_stats = get_sqs_queue_stats(args.status_queue)
        
        # Display queue information
        print("QUEUE STATUS:")
        print("-" * 80)
        if task_queue_stats:
            print(f"Task Queue: {args.sqs_queue}")
            print(f"  - Visible Messages: {task_queue_stats['ApproximateNumberOfMessages']}")
            print(f"  - In-Flight Messages: {task_queue_stats['ApproximateNumberOfMessagesNotVisible']}")
            print(f"  - Delayed Messages: {task_queue_stats['ApproximateNumberOfMessagesDelayed']}")
            print(f"  - Total Messages: {task_queue_stats['TotalMessages']}")
        else:
            print(f"Task Queue: {args.sqs_queue} - UNAVAILABLE")
        
        print()
        
        if status_queue_stats:
            print(f"Status Queue: {args.status_queue}")
            print(f"  - Visible Messages: {status_queue_stats['ApproximateNumberOfMessages']}")
            print(f"  - In-Flight Messages: {status_queue_stats['ApproximateNumberOfMessagesNotVisible']}")
            print(f"  - Delayed Messages: {status_queue_stats['ApproximateNumberOfMessagesDelayed']}")
            print(f"  - Total Messages: {status_queue_stats['TotalMessages']}")
        else:
            print(f"Status Queue: {args.status_queue} - UNAVAILABLE")
        
        print("\nEC2 INSTANCES:")
        print("-" * 80)
        instances = get_ec2_instance_info(args.stack_name)
        if instances:
            for i, instance in enumerate(instances):
                print(f"Instance {i+1}: {instance['Name']} ({instance['InstanceId']})")
                print(f"  - State: {instance['State']}")
                print(f"  - Type: {instance['InstanceType']}")
                print(f"  - Public IP: {instance['PublicIpAddress']}")
                print(f"  - Public DNS: {instance['PublicDnsName']}")
                print(f"  - Launch Time: {instance['LaunchTime']}")
                print()
        else:
            print("No instances found or unable to retrieve instance information.")
        
        print("\nSTORAGE STATUS:")
        print("-" * 80)
        bucket_info = check_s3_bucket(args.s3_bucket)
        if bucket_info:
            print(f"S3 Bucket: {bucket_info['BucketName']}")
            print(f"  - Object Count: {bucket_info['ObjectCount']}")
            if bucket_info['ObjectCount'] > 0:
                print("  - Sample Objects:")
                for obj in bucket_info['SampleObjects'][:5]:
                    print(f"    - {obj}")
                if len(bucket_info['SampleObjects']) > 5:
                    print(f"    - ... and {len(bucket_info['SampleObjects']) - 5} more")
        else:
            print(f"S3 Bucket: {args.s3_bucket} - UNAVAILABLE")
        
        print("\nACTIONS:")
        print("-" * 80)
        if task_queue_stats and task_queue_stats['ApproximateNumberOfMessages'] == 0:
            print("* Consider adding URLs to the task queue: python debug_queue.py add")
        
        if not instances or all(instance['State'] != 'running' for instance in instances):
            print("* No running crawler instances. Deploy using: python deploy_simple_crawler.py --key-name YOUR_KEY_NAME")
        
        print("\n" + "="*80 + "\n")
    
    if args.continuous:
        try:
            while True:
                display_status()
                print(f"Next update in {args.interval} seconds. Press Ctrl+C to exit.")
                time.sleep(args.interval)
        except KeyboardInterrupt:
            print("\nMonitoring stopped.")
    else:
        display_status()

if __name__ == "__main__":
    main() 