#!/usr/bin/env python3
import boto3
import json
import time
import logging
import base64

def launch_crawler_instances():
    """Launch EC2 instances for the web crawler system with public access"""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Load configuration
    with open('aws_config.json', 'r') as f:
        config = json.load(f)
    
    region = config['aws']['region']
    bucket_name = config['aws']['bucket_name']
    
    # Initialize EC2 client
    ec2 = boto3.client('ec2', region_name=region)
    
    # Get security group ID
    sg_name = 'crawler-sg'
    try:
        response = ec2.describe_security_groups(GroupNames=[sg_name])
        sg_id = response['SecurityGroups'][0]['GroupId']
    except Exception as e:
        logging.error(f"Security group {sg_name} not found: {str(e)}")
        return
    
    # Create user data scripts for different node types
    user_data_base = """#!/bin/bash
# Update system
sudo apt-get update
sudo apt-get upgrade -y

# Install dependencies
sudo apt-get install -y python3-pip python3-dev git build-essential wget

# Create working directory
mkdir -p /home/ubuntu/crawler
cd /home/ubuntu/crawler

# Download bootstrap script from S3
echo "Downloading bootstrap script from S3..."
wget "https://{bucket_name}.s3.amazonaws.com/code/bootstrap.sh"
chmod +x bootstrap.sh

# Run bootstrap script
./bootstrap.sh

# Configure AWS region
mkdir -p /home/ubuntu/.aws
cat > /home/ubuntu/.aws/config << EOF
[default]
region = {region}
EOF

# Set node role and other parameters
"""

    master_specific = """
# Additional setup for master node
echo "Setting up master node"
# Create log directory
mkdir -p logs

# Start the master node
python3 main.py --role master --sqs-queue crawler-url-queue --status-queue crawler-status-queue --bucket {bucket_name} > logs/master.log 2>&1 &
echo "Master node started"
"""

    indexer_specific = """
# Additional setup for indexer node
echo "Setting up indexer node"
# Create directories
mkdir -p logs
mkdir -p index

# Start the indexer node
python3 main.py --role indexer --status-queue crawler-status-queue --bucket {bucket_name} > logs/indexer.log 2>&1 &
echo "Indexer node started"
"""

    crawler_specific = """
# Additional setup for crawler node
echo "Setting up crawler node"
# Create log directory
mkdir -p logs

# Start the crawler node
python3 main.py --role crawler --sqs-queue crawler-url-queue --status-queue crawler-status-queue --bucket {bucket_name} > logs/crawler.log 2>&1 &
echo "Crawler node started"
"""

    # Format the user data scripts with actual region and bucket
    master_user_data = user_data_base.format(region=region, bucket_name=bucket_name) + master_specific.format(bucket_name=bucket_name)
    indexer_user_data = user_data_base.format(region=region, bucket_name=bucket_name) + indexer_specific.format(bucket_name=bucket_name)
    crawler_user_data = user_data_base.format(region=region, bucket_name=bucket_name) + crawler_specific.format(bucket_name=bucket_name)
    
    # Encode user data
    master_user_data_encoded = base64.b64encode(master_user_data.encode()).decode()
    indexer_user_data_encoded = base64.b64encode(indexer_user_data.encode()).decode()
    crawler_user_data_encoded = base64.b64encode(crawler_user_data.encode()).decode()
    
    # Find a free tier eligible Ubuntu AMI (Ubuntu 20.04 LTS)
    try:
        # Get the latest Ubuntu 20.04 LTS AMI (free tier eligible)
        logging.info("Searching for the latest Ubuntu 20.04 LTS AMI...")
        response = ec2.describe_images(
            Owners=['099720109477'],  # Canonical's owner ID
            Filters=[
                {'Name': 'name', 'Values': ['ubuntu/images/hvm-ssd/ubuntu-focal-20.04-amd64-server-*']},
                {'Name': 'state', 'Values': ['available']},
                {'Name': 'virtualization-type', 'Values': ['hvm']}
            ]
        )
        
        # Sort by creation date to get the latest
        if not response['Images']:
            logging.warning("No Ubuntu 20.04 images found, trying Amazon Linux 2 instead...")
            response = ec2.describe_images(
                Owners=['amazon'],
                Filters=[
                    {'Name': 'name', 'Values': ['amzn2-ami-hvm-*-x86_64-gp2']},
                    {'Name': 'state', 'Values': ['available']}
                ]
            )
        
        images = sorted(response['Images'], key=lambda x: x['CreationDate'], reverse=True)
        if not images:
            logging.error("No suitable AMI found")
            return
        
        ami_id = images[0]['ImageId']
        ami_name = images[0]['Name']
        logging.info(f"Using AMI: {ami_id} ({ami_name})")
    except Exception as e:
        logging.error(f"Error finding AMI: {str(e)}")
        return
    
    # Launch instance configurations
    instance_configs = [
        {'name': 'master-node', 'user_data': master_user_data_encoded, 'type': 't2.micro', 'role': 'master'},
        {'name': 'indexer-node', 'user_data': indexer_user_data_encoded, 'type': 't2.micro', 'role': 'indexer'},
        {'name': 'crawler-node', 'user_data': crawler_user_data_encoded, 'type': 't2.micro', 'role': 'crawler'}
    ]
    
    # Launch instances
    launched_instances = []
    
    for config in instance_configs:
        try:
            logging.info(f"Launching {config['name']} instance...")
            # Try to use instance profile if it exists
            try:
                response = ec2.run_instances(
                    ImageId=ami_id,
                    InstanceType=config['type'],
                    MinCount=1,
                    MaxCount=1,
                    KeyName='crawler-key',
                    SecurityGroupIds=[sg_id],
                    UserData=config['user_data'],
                    IamInstanceProfile={
                        'Name': 'CrawlerInstanceProfile'
                    },
                    TagSpecifications=[
                        {
                            'ResourceType': 'instance',
                            'Tags': [
                                {'Key': 'Name', 'Value': config['name']},
                                {'Key': 'Role', 'Value': config['role']}
                            ]
                        }
                    ],
                    # Use default subnet which is in the default VPC and public
                    InstanceInitiatedShutdownBehavior='terminate'  # Auto-terminate on system shutdown
                )
            except Exception as profile_error:
                logging.warning(f"Could not launch with instance profile: {str(profile_error)}")
                logging.warning("Launching without instance profile...")
                # If instance profile doesn't exist, launch without it
                response = ec2.run_instances(
                    ImageId=ami_id,
                    InstanceType=config['type'],
                    MinCount=1,
                    MaxCount=1,
                    KeyName='crawler-key',
                    SecurityGroupIds=[sg_id],
                    UserData=config['user_data'],
                    TagSpecifications=[
                        {
                            'ResourceType': 'instance',
                            'Tags': [
                                {'Key': 'Name', 'Value': config['name']},
                                {'Key': 'Role', 'Value': config['role']}
                            ]
                        }
                    ],
                    # Use default subnet which is in the default VPC and public
                    InstanceInitiatedShutdownBehavior='terminate'  # Auto-terminate on system shutdown
                )
            
            instance_id = response['Instances'][0]['InstanceId']
            launched_instances.append((instance_id, config['name']))
            logging.info(f"Launched {config['name']} with ID: {instance_id}")
            
        except Exception as e:
            logging.error(f"Error launching {config['name']}: {str(e)}")
    
    # Wait for instances to be running
    if launched_instances:
        logging.info("Waiting for instances to be in running state...")
        instance_ids = [instance[0] for instance in launched_instances]
        
        waiter = ec2.get_waiter('instance_running')
        waiter.wait(InstanceIds=instance_ids)
        
        # Get public IPs
        response = ec2.describe_instances(InstanceIds=instance_ids)
        
        for reservation in response['Reservations']:
            for instance in reservation['Instances']:
                instance_id = instance['InstanceId']
                public_ip = instance.get('PublicIpAddress', 'N/A')
                public_dns = instance.get('PublicDnsName', 'N/A')
                
                # Find the instance name
                instance_name = next((name for id, name in launched_instances if id == instance_id), 'Unknown')
                
                logging.info(f"{instance_name} ({instance_id}):")
                logging.info(f"  Public IP: {public_ip}")
                logging.info(f"  Public DNS: {public_dns}")
                logging.info(f"  SSH command: ssh -i crawler-key.pem ubuntu@{public_ip}")
                print(f"{instance_name} ({instance_id}):")
                print(f"  Public IP: {public_ip}")
                print(f"  Public DNS: {public_dns}")
                print(f"  SSH command: ssh -i crawler-key.pem ubuntu@{public_ip}")
                print("----------------------------------------------------")
    
    logging.info("Instance launch process complete!")

if __name__ == "__main__":
    launch_crawler_instances() 