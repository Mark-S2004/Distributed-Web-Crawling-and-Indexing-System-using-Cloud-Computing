#!/usr/bin/env python3
import boto3
import json
import logging
import argparse
import time
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def check_instances():
    """Check running EC2 instances that are part of the crawler system"""
    ec2 = boto3.client('ec2')
    autoscaling = boto3.client('autoscaling')
    
    # Check autoscaling group
    logging.info("Checking Auto Scaling Groups...")
    try:
        response = autoscaling.describe_auto_scaling_groups(
            AutoScalingGroupNames=['crawler-auto-scaling-group']
        )
        if response['AutoScalingGroups']:
            asg = response['AutoScalingGroups'][0]
            logging.info(f"Auto Scaling Group: crawler-auto-scaling-group")
            logging.info(f"  Min Size: {asg['MinSize']}")
            logging.info(f"  Max Size: {asg['MaxSize']}")
            logging.info(f"  Desired Capacity: {asg['DesiredCapacity']}")
            logging.info(f"  Instances: {len(asg['Instances'])}")
            
            instance_ids = [i['InstanceId'] for i in asg['Instances']]
            if instance_ids:
                logging.info(f"  Instance IDs: {', '.join(instance_ids)}")
            else:
                logging.warning("  No instances in Auto Scaling Group!")
        else:
            logging.warning("Auto Scaling Group 'crawler-auto-scaling-group' not found!")
    except Exception as e:
        logging.error(f"Error checking Auto Scaling Group: {e}")
    
    # Check all EC2 instances with crawler tags
    logging.info("\nChecking all EC2 instances with crawler tags...")
    try:
        response = ec2.describe_instances(
            Filters=[
                {'Name': 'tag:Role', 'Values': ['crawler']}
            ]
        )
        instances = []
        for reservation in response['Reservations']:
            for instance in reservation['Instances']:
                instances.append({
                    'InstanceId': instance['InstanceId'],
                    'State': instance['State']['Name'],
                    'InstanceType': instance.get('InstanceType', 'N/A'),
                    'LaunchTime': instance.get('LaunchTime', 'N/A')
                })
        
        if instances:
            logging.info(f"Found {len(instances)} crawler instances:")
            for i, instance in enumerate(instances, 1):
                logging.info(f"  {i}. {instance['InstanceId']} - {instance['State']} - {instance['InstanceType']} - Launched: {instance['LaunchTime']}")
        else:
            logging.warning("No crawler instances found!")
    except Exception as e:
        logging.error(f"Error checking EC2 instances: {e}")

def check_queues(sqs_queue='crawler-url-queue', status_queue='crawler-status-queue'):
    """Check SQS queues status and sample messages"""
    sqs = boto3.client('sqs')
    
    # Check main queue
    logging.info(f"\nChecking URL queue: {sqs_queue}")
    try:
        # Get queue URL
        response = sqs.get_queue_url(QueueName=sqs_queue)
        queue_url = response['QueueUrl']
        
        # Get queue attributes
        attr_response = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=['All']
        )
        attrs = attr_response['Attributes']
        logging.info(f"  Queue URL: {queue_url}")
        logging.info(f"  Messages Available: {attrs.get('ApproximateNumberOfMessages', 'N/A')}")
        logging.info(f"  Messages In Flight: {attrs.get('ApproximateNumberOfMessagesNotVisible', 'N/A')}")
        
        # Sample some messages
        logging.info("\nSampling messages from queue (without removing)...")
        sample_response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=5,
            VisibilityTimeout=5,  # Short timeout to return to queue quickly
            WaitTimeSeconds=1
        )
        
        messages = sample_response.get('Messages', [])
        if messages:
            logging.info(f"  Retrieved {len(messages)} sample messages:")
            for i, msg in enumerate(messages, 1):
                body = msg.get('Body', '')
                if body.startswith('{'):
                    try:
                        # Try to parse as JSON
                        body_json = json.loads(body)
                        if 'type' in body_json and body_json['type'] == 'urls_discovered':
                            discovered_urls = body_json.get('urls', [])
                            logging.info(f"  {i}. MSG TYPE: urls_discovered, URL Count: {len(discovered_urls)}")
                            logging.info(f"     Source URL: {body_json.get('url', 'N/A')}")
                            if discovered_urls:
                                sample_urls = discovered_urls[:3]
                                logging.info(f"     Sample URLs: {sample_urls}")
                        else:
                            logging.info(f"  {i}. MSG TYPE: {body_json.get('type', 'unknown')}")
                    except json.JSONDecodeError:
                        # Not valid JSON, just show the first part
                        logging.info(f"  {i}. Raw message: {body[:100]}...")
                else:
                    # Likely a raw URL
                    logging.info(f"  {i}. URL: {body}")
        else:
            logging.warning("  No messages available for sampling")
            
        # Don't forget to make messages visible again
        for msg in messages:
            sqs.change_message_visibility(
                QueueUrl=queue_url,
                ReceiptHandle=msg['ReceiptHandle'],
                VisibilityTimeout=0  # Make immediately visible again
            )
    except Exception as e:
        logging.error(f"Error checking SQS queue {sqs_queue}: {e}")
    
    # Check status queue
    logging.info(f"\nChecking status queue: {status_queue}")
    try:
        # Get queue URL
        response = sqs.get_queue_url(QueueName=status_queue)
        status_queue_url = response['QueueUrl']
        
        # Get queue attributes
        attr_response = sqs.get_queue_attributes(
            QueueUrl=status_queue_url,
            AttributeNames=['All']
        )
        attrs = attr_response['Attributes']
        logging.info(f"  Queue URL: {status_queue_url}")
        logging.info(f"  Messages Available: {attrs.get('ApproximateNumberOfMessages', 'N/A')}")
        logging.info(f"  Messages In Flight: {attrs.get('ApproximateNumberOfMessagesNotVisible', 'N/A')}")
    except Exception as e:
        logging.error(f"Error checking status queue {status_queue}: {e}")

def check_purge_queue(sqs_queue='crawler-url-queue', purge=False):
    """Check and optionally purge the queue"""
    sqs = boto3.client('sqs')
    
    if purge:
        try:
            # Get queue URL
            response = sqs.get_queue_url(QueueName=sqs_queue)
            queue_url = response['QueueUrl']
            
            # Confirm with user
            confirm = input(f"Are you sure you want to purge all messages from {sqs_queue}? (yes/no): ")
            if confirm.lower() == 'yes':
                sqs.purge_queue(QueueUrl=queue_url)
                logging.info(f"Queue {sqs_queue} purged successfully")
            else:
                logging.info("Purge operation cancelled")
        except Exception as e:
            logging.error(f"Error purging queue {sqs_queue}: {e}")

def push_test_urls(sqs_queue='crawler-url-queue', count=10):
    """Push test URLs to the queue for testing"""
    sqs = boto3.client('sqs')
    
    try:
        # Get queue URL
        response = sqs.get_queue_url(QueueName=sqs_queue)
        queue_url = response['QueueUrl']
        
        # Confirm with user
        confirm = input(f"Are you sure you want to push {count} test URLs to {sqs_queue}? (yes/no): ")
        if confirm.lower() == 'yes':
            test_sites = [
                "https://www.python.org",
                "https://en.wikipedia.org/wiki/Python_(programming_language)",
                "https://github.com/python",
                "https://stackoverflow.com/questions/tagged/python",
                "https://docs.python.org/3/",
                "https://pypi.org/",
                "https://www.djangoproject.com/",
                "https://flask.palletsprojects.com/",
                "https://aws.amazon.com/",
                "https://cloud.google.com/",
                "https://azure.microsoft.com/",
                "https://www.reddit.com/r/Python/",
                "https://news.ycombinator.com/",
                "https://www.bbc.com/",
                "https://www.nytimes.com/"
            ]
            
            # Make sure we have enough URLs
            while len(test_sites) < count:
                test_sites.extend(test_sites)
            
            # Send URLs to the queue
            sent_count = 0
            for i in range(count):
                url = test_sites[i % len(test_sites)]
                sqs.send_message(
                    QueueUrl=queue_url,
                    MessageBody=url
                )
                sent_count += 1
                if sent_count % 10 == 0:
                    logging.info(f"Sent {sent_count} URLs...")
            
            logging.info(f"Successfully sent {sent_count} test URLs to {sqs_queue}")
        else:
            logging.info("Operation cancelled")
    except Exception as e:
        logging.error(f"Error sending test URLs to {sqs_queue}: {e}")

def main():
    parser = argparse.ArgumentParser(description='Check crawler system status')
    parser.add_argument('--check-instances', action='store_true', help='Check EC2 instances')
    parser.add_argument('--check-queues', action='store_true', help='Check SQS queues')
    parser.add_argument('--sqs-queue', default='crawler-url-queue', help='Name of the URL queue')
    parser.add_argument('--status-queue', default='crawler-status-queue', help='Name of the status queue')
    parser.add_argument('--purge-queue', action='store_true', help='Purge the URL queue')
    parser.add_argument('--push-test-urls', type=int, default=0, help='Push test URLs to the queue')
    
    args = parser.parse_args()
    
    # If no specific actions are specified, check both instances and queues
    if not (args.check_instances or args.check_queues or args.purge_queue or args.push_test_urls):
        args.check_instances = True
        args.check_queues = True
    
    if args.check_instances:
        check_instances()
    
    if args.check_queues:
        check_queues(args.sqs_queue, args.status_queue)
    
    if args.purge_queue:
        check_purge_queue(args.sqs_queue, purge=True)
    
    if args.push_test_urls > 0:
        push_test_urls(args.sqs_queue, args.push_test_urls)

if __name__ == "__main__":
    main() 