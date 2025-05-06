#!/usr/bin/env python3
"""
EC2 Deployment Script for Web Crawler Interface

This script helps deploy the web crawler interface to an EC2 instance.
It creates a new EC2 instance, sets up the necessary dependencies,
and configures the web interface to run on startup.
"""

import os
import sys
import time
import argparse
import boto3
import paramiko
import getpass
from botocore.exceptions import ClientError

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Deploy web crawler interface to EC2')
    parser.add_argument('--key-name', required=True, help='Name of the EC2 key pair to use')
    parser.add_argument('--key-file', required=True, help='Path to the private key file (.pem)')
    parser.add_argument('--instance-type', default='t2.micro', help='EC2 instance type (default: t2.micro)')
    parser.add_argument('--region', default='us-east-1', help='AWS region (default: us-east-1)')
    parser.add_argument('--security-group', default='crawler-sg', help='Security group name (default: crawler-sg)')
    parser.add_argument('--instance-name', default='web-crawler-interface', help='Name tag for the instance')
    return parser.parse_args()

def get_security_group(ec2_client, sg_name):
    """Get the existing security group or create a new one with all ports open."""
    try:
        # Check if security group already exists
        response = ec2_client.describe_security_groups(
            Filters=[{'Name': 'group-name', 'Values': [sg_name]}]
        )
        if response['SecurityGroups']:
            print(f"Using existing security group '{sg_name}'.")
            sg_id = response['SecurityGroups'][0]['GroupId']

            # Make sure port 5000 is open for the web interface
            try:
                print(f"Ensuring port 5000 is open in security group '{sg_name}'...")
                ec2_client.authorize_security_group_ingress(
                    GroupId=sg_id,
                    IpPermissions=[
                        # Web interface access
                        {
                            'IpProtocol': 'tcp',
                            'FromPort': 5000,
                            'ToPort': 5000,
                            'IpRanges': [{'CidrIp': '0.0.0.0/0'}]
                        }
                    ]
                )
            except ClientError as e:
                # Ignore if the rule already exists
                if 'InvalidPermission.Duplicate' not in str(e):
                    print(f"Warning: {e}")

            return sg_id

        # Create security group if it doesn't exist
        print(f"Creating security group '{sg_name}'...")
        response = ec2_client.create_security_group(
            GroupName=sg_name,
            Description='Security group for web crawler with all connections public'
        )
        sg_id = response['GroupId']

        # Add inbound rules - allow all traffic
        ec2_client.authorize_security_group_ingress(
            GroupId=sg_id,
            IpPermissions=[
                # Allow all TCP
                {
                    'IpProtocol': 'tcp',
                    'FromPort': 0,
                    'ToPort': 65535,
                    'IpRanges': [{'CidrIp': '0.0.0.0/0'}]
                },
                # Allow all UDP
                {
                    'IpProtocol': 'udp',
                    'FromPort': 0,
                    'ToPort': 65535,
                    'IpRanges': [{'CidrIp': '0.0.0.0/0'}]
                },
                # Allow ICMP
                {
                    'IpProtocol': 'icmp',
                    'FromPort': -1,
                    'ToPort': -1,
                    'IpRanges': [{'CidrIp': '0.0.0.0/0'}]
                }
            ]
        )
        print(f"Security group created with ID: {sg_id}")
        return sg_id
    except ClientError as e:
        print(f"Error with security group: {e}")
        sys.exit(1)

def create_ec2_instance(ec2_resource, args, sg_id):
    """Create a new EC2 instance for the web interface with public access."""
    try:
        print(f"Creating EC2 instance '{args.instance_name}'...")

        # Get the latest Amazon Linux 2 AMI
        ec2_client = ec2_resource.meta.client
        response = ec2_client.describe_images(
            Owners=['amazon'],
            Filters=[
                {'Name': 'name', 'Values': ['amzn2-ami-hvm-*-x86_64-gp2']},
                {'Name': 'state', 'Values': ['available']}
            ]
        )

        # Sort by creation date to get the latest AMI
        amis = sorted(response['Images'], key=lambda x: x['CreationDate'], reverse=True)
        if not amis:
            print("No Amazon Linux 2 AMI found.")
            sys.exit(1)

        ami_id = amis[0]['ImageId']
        print(f"Using AMI: {ami_id}")

        # Create the instance with public IP address
        instances = ec2_resource.create_instances(
            ImageId=ami_id,
            InstanceType=args.instance_type,
            KeyName=args.key_name,
            MinCount=1,
            MaxCount=1,
            # Use NetworkInterfaces to specify security group and public IP
            NetworkInterfaces=[{
                'DeviceIndex': 0,
                'AssociatePublicIpAddress': True,
                'DeleteOnTermination': True,
                'Groups': [sg_id]
            }],
            TagSpecifications=[
                {
                    'ResourceType': 'instance',
                    'Tags': [
                        {'Key': 'Name', 'Value': args.instance_name},
                        {'Key': 'Project', 'Value': 'WebCrawler'},
                        {'Key': 'Role', 'Value': 'WebInterface'}
                    ]
                }
            ],
            UserData='''#!/bin/bash
# Install dependencies
yum update -y
yum install -y python3 python3-pip git
pip3 install flask boto3 whoosh paramiko awscli

# Create directory for the application
mkdir -p /opt/web-crawler

# Set up AWS CLI to use IMDSv2 for metadata
mkdir -p /root/.aws
echo "[default]" > /root/.aws/config
echo "region = us-east-1" >> /root/.aws/config
echo "output = json" >> /root/.aws/config
'''
        )

        instance = instances[0]
        print(f"Instance created with ID: {instance.id}")

        # Wait for the instance to be running
        print("Waiting for instance to start...")
        instance.wait_until_running()

        # Reload the instance to get the public IP
        instance.reload()
        public_ip = instance.public_ip_address

        if not public_ip:
            print("Warning: Instance started but no public IP was assigned.")
            print("Make sure your VPC and subnet settings allow public IP assignment.")
            # Try to allocate an Elastic IP if no public IP was assigned
            try:
                print("Attempting to allocate an Elastic IP...")
                eip = ec2_client.allocate_address(Domain='vpc')
                ec2_client.associate_address(
                    InstanceId=instance.id,
                    AllocationId=eip['AllocationId']
                )
                print(f"Elastic IP allocated: {eip['PublicIp']}")
                public_ip = eip['PublicIp']
            except Exception as e:
                print(f"Failed to allocate Elastic IP: {e}")

        print(f"Instance is running with public IP: {public_ip}")

        return instance
    except ClientError as e:
        print(f"Error creating EC2 instance: {e}")
        sys.exit(1)

def wait_for_ssh(host, username, key_file, timeout=300):
    """Wait for SSH to be available on the instance."""
    print(f"Waiting for SSH to be available on {host}...")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            ssh.connect(hostname=host, username=username, key_filename=key_file, timeout=5)
            print("SSH is available!")
            ssh.close()
            return True
        except Exception as e:
            print(f"SSH not yet available: {e}")
            time.sleep(10)

    print(f"Timed out waiting for SSH after {timeout} seconds.")
    return False

def upload_files(host, username, key_file):
    """Upload the web interface files to the EC2 instance."""
    try:
        print(f"Uploading files to {host}...")

        # Create SSH client
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=host, username=username, key_filename=key_file)

        # Create SFTP client
        sftp = ssh.open_sftp()

        # Create remote directory
        _, stdout, _ = ssh.exec_command('mkdir -p /opt/web-crawler')
        stdout.channel.recv_exit_status()

        # Check if any required files exist
        found_files = False

        # Try to upload web_search.py (the main file)
        if os.path.exists('web_search.py'):
            print(f"Uploading web_search.py to /opt/web-crawler/web_search.py")
            sftp.put('web_search.py', '/opt/web-crawler/web_search.py')
            found_files = True
        else:
            print("Error: web_search.py not found in current directory")

        # Try to upload other supporting files if they exist
        for file in ['indexerNode.py', 'cloud_queue.py', 'db_manager.py']:
            if os.path.exists(file):
                print(f"Uploading {file} to /opt/web-crawler/{file}")
                sftp.put(file, f'/opt/web-crawler/{file}')
                found_files = True

        # If no files were found, return an error
        if not found_files:
            print("Error: No required files found in current directory")
            return False

        # Set up systemd service for auto-start
        service_file = '''[Unit]
Description=Web Crawler Interface
After=network.target

[Service]
User=ec2-user
WorkingDirectory=/opt/web-crawler
ExecStart=/usr/bin/python3 /opt/web-crawler/web_search.py --host 0.0.0.0 --port 5000
Restart=always

[Install]
WantedBy=multi-user.target
'''

        # Create temporary service file
        with open('web-crawler.service', 'w') as f:
            f.write(service_file)

        # Upload service file
        sftp.put('web-crawler.service', '/tmp/web-crawler.service')

        # Move service file to systemd directory and enable it
        commands = [
            'sudo mv /tmp/web-crawler.service /etc/systemd/system/',
            'sudo systemctl daemon-reload',
            'sudo systemctl enable web-crawler.service',
            'sudo systemctl start web-crawler.service'
        ]

        for cmd in commands:
            print(f"Executing: {cmd}")
            _, stdout, stderr = ssh.exec_command(cmd)
            exit_status = stdout.channel.recv_exit_status()
            if exit_status != 0:
                print(f"Command failed with status {exit_status}")
                print(f"Error: {stderr.read().decode('utf-8')}")

        # Clean up
        os.remove('web-crawler.service')

        # Close connections
        sftp.close()
        ssh.close()

        print("Files uploaded and service configured successfully!")
        return True
    except Exception as e:
        print(f"Error uploading files: {e}")
        return False

def main():
    """Main function to deploy the web interface to EC2."""
    args = parse_args()

    # Check if key file exists
    if not os.path.isfile(args.key_file):
        print(f"Key file not found: {args.key_file}")
        sys.exit(1)

    # Check if we're in the correct directory
    if not os.path.exists('web_search.py'):
        print("Error: web_search.py not found in current directory.")
        print("Make sure you're running this script from the same directory as web_search.py")
        print(f"Current directory: {os.getcwd()}")
        print("Files in current directory:")
        for file in os.listdir('.'):
            if os.path.isfile(file):
                print(f"  - {file}")
        sys.exit(1)

    # On Unix-like systems, check key file permissions
    if os.name == 'posix':
        key_permissions = oct(os.stat(args.key_file).st_mode & 0o777)
        if key_permissions != '0o600':
            print(f"Warning: Key file permissions are {key_permissions}, should be 0o600")
            print("Attempting to set correct permissions...")
            try:
                os.chmod(args.key_file, 0o600)
                print("Key file permissions set to 0o600")
            except Exception as e:
                print(f"Failed to set key file permissions: {e}")
                print("You may need to manually set permissions: chmod 600 " + args.key_file)

    # Create AWS clients
    try:
        ec2_client = boto3.client('ec2', region_name=args.region)
        ec2_resource = boto3.resource('ec2', region_name=args.region)
    except Exception as e:
        print(f"Error connecting to AWS: {e}")
        sys.exit(1)

    # Get or create security group
    sg_id = get_security_group(ec2_client, args.security_group)

    # Create EC2 instance
    instance = create_ec2_instance(ec2_resource, args, sg_id)

    # Wait for SSH to be available
    if not wait_for_ssh(instance.public_ip_address, 'ec2-user', args.key_file):
        print("Failed to connect to the instance via SSH.")
        sys.exit(1)

    # Upload files
    if not upload_files(instance.public_ip_address, 'ec2-user', args.key_file):
        print("Failed to upload files to the instance.")
        sys.exit(1)

    print("\n=== Deployment Complete ===")
    print(f"Web interface is available at: http://{instance.public_ip_address}:5000")
    print("The interface will automatically start when the instance boots.")
    print(f"To SSH into the instance: ssh -i {args.key_file} ec2-user@{instance.public_ip_address}")
    print("\nIMPORTANT: Make sure to configure your AWS credentials on the EC2 instance.")
    print("You can do this by SSH into the instance and running 'aws configure'.")

if __name__ == '__main__':
    main()
