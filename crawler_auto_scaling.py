#!/usr/bin/env python3
import boto3
import json
import time
import logging
import base64
import os
import argparse

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def upload_latest_code(bucket_name, region):
    """Upload latest code files to S3 bucket"""
    logging.info(f"Uploading latest code to S3 bucket: {bucket_name}")

    # Initialize S3 client
    s3 = boto3.client('s3', region_name=region)

    # Create bucket if it doesn't exist
    try:
        s3.head_bucket(Bucket=bucket_name)
        logging.info(f"Bucket {bucket_name} already exists")
    except Exception:
        logging.info(f"Creating bucket {bucket_name}...")
        if region == 'us-east-1':
            s3.create_bucket(Bucket=bucket_name)
        else:
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint': region}
            )
        logging.info(f"Bucket {bucket_name} created")

    # Set bucket policy for public access
    try:
        bucket_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "PublicReadForGetBucketObjects",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": f"arn:aws:s3:::{bucket_name}/*"
                }
            ]
        }

        s3.put_bucket_policy(
            Bucket=bucket_name,
            Policy=json.dumps(bucket_policy)
        )
        logging.info("Set bucket policy for public access")
    except Exception as e:
        logging.warning(f"Could not set bucket policy: {e}")

    # Key files needed for crawler node - expanded to include all necessary files
    files_to_upload = [
        'main.py',
        'crawlerNode.py',
        'masterNode.py',
        'indexerNode.py',
        'cloud_queue.py',
        'cloud_storage.py',
        'db_manager.py',
        'requirements.txt'
    ]

    # Create a timestamp file to force updates
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    with open('update_timestamp.txt', 'w') as f:
        f.write(f"Update timestamp: {timestamp}\n")
        f.write("This file is used to force crawler nodes to update their code.\n")

    files_to_upload.append('update_timestamp.txt')

    # Ensure we're using the latest version of these files
    logging.info(f"Preparing to upload files with timestamp {timestamp}")

    # Create requirements.txt if it doesn't exist
    if not os.path.exists('requirements.txt'):
        with open('requirements.txt', 'w') as f:
            f.write("boto3\nrequests\nbeautifulsoup4\n")
        logging.info("Created requirements.txt with basic dependencies")

    # Upload each file
    for filename in files_to_upload:
        if os.path.exists(filename):
            try:
                logging.info(f"Uploading {filename} to S3...")
                # Try to upload with public-read ACL directly
                try:
                    s3.upload_file(
                        filename,
                        bucket_name,
                        f"code/{filename}",
                        ExtraArgs={'ACL': 'public-read'}
                    )
                except Exception as acl_error:
                    if 'AccessControlListNotSupported' in str(acl_error):
                        # If ACLs not supported, upload without ACL
                        logging.info(f"Bucket doesn't support ACLs, uploading {filename} without ACL...")
                        s3.upload_file(
                            filename,
                            bucket_name,
                            f"code/{filename}"
                        )
                    else:
                        raise acl_error

                # Get the public URL
                url = f"https://{bucket_name}.s3.amazonaws.com/code/{filename}"
                logging.info(f"Uploaded {filename} to {url}")
            except Exception as e:
                logging.error(f"Error uploading {filename}: {e}")
        else:
            logging.warning(f"File {filename} not found, skipping")

    # Create bootstrap script for downloading from S3
    bootstrap_script = f"""#!/bin/bash
# Script to download crawler code from S3 bucket

# Create working directory
mkdir -p /home/ec2-user/crawler
cd /home/ec2-user/crawler

# Download code files from S3
FILES=(
    "main.py"
    "crawlerNode.py"
    "masterNode.py"
    "indexerNode.py"
    "cloud_queue.py"
    "cloud_storage.py"
    "db_manager.py"
    "requirements.txt"
    "update_timestamp.txt"
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

    # Upload bootstrap script
    try:
        logging.info("Creating bootstrap script in S3...")
        # Try with public-read ACL
        try:
            s3.put_object(
                Bucket=bucket_name,
                Key="code/bootstrap.sh",
                Body=bootstrap_script,
                ContentType='text/plain',
                ACL='public-read'
            )
        except Exception as acl_error:
            if 'AccessControlListNotSupported' in str(acl_error):
                # If ACLs not supported, try without ACL
                logging.info("Bucket doesn't support ACLs, uploading bootstrap script without ACL...")
                s3.put_object(
                    Bucket=bucket_name,
                    Key="code/bootstrap.sh",
                    Body=bootstrap_script,
                    ContentType='text/plain'
                )
            else:
                raise acl_error

        bootstrap_url = f"https://{bucket_name}.s3.amazonaws.com/code/bootstrap.sh"
        logging.info(f"Bootstrap script available at: {bootstrap_url}")
    except Exception as e:
        logging.error(f"Error creating bootstrap script: {e}")

    return True

def create_security_group(ec2, sg_name="crawler-sg"):
    """Create or get security group for crawler instances"""
    sg_id = None

    try:
        # Try to get existing security group
        response = ec2.describe_security_groups(GroupNames=[sg_name])
        sg_id = response['SecurityGroups'][0]['GroupId']
        logging.info(f"Using existing security group: {sg_id}")
    except Exception:
        # Create new security group
        logging.info(f"Creating new security group: {sg_name}")
        vpc_response = ec2.describe_vpcs()
        vpc_id = vpc_response['Vpcs'][0]['VpcId']

        try:
            sg_response = ec2.create_security_group(
                GroupName=sg_name,
                Description='Security group for crawler nodes',
                VpcId=vpc_id
            )
            sg_id = sg_response['GroupId']

            # Add permissive rules
            ec2.authorize_security_group_ingress(
                GroupId=sg_id,
                IpPermissions=[
                    {
                        'IpProtocol': 'tcp',
                        'FromPort': 22,
                        'ToPort': 22,
                        'IpRanges': [{'CidrIp': '0.0.0.0/0'}]
                    },
                    {
                        'IpProtocol': 'tcp',
                        'FromPort': 80,
                        'ToPort': 80,
                        'IpRanges': [{'CidrIp': '0.0.0.0/0'}]
                    },
                    {
                        'IpProtocol': 'tcp',
                        'FromPort': 443,
                        'ToPort': 443,
                        'IpRanges': [{'CidrIp': '0.0.0.0/0'}]
                    }
                ]
            )
            logging.info(f"Created security group: {sg_id}")
        except Exception as e:
            logging.error(f"Failed to create security group: {e}")
            return None

    return sg_id

def setup_iam_role(iam):
    """Set up IAM role and instance profile for crawler with proper permissions"""
    role_name = 'CrawlerInstanceRole'
    instance_profile_name = 'CrawlerInstanceProfile'

    # Create role if it doesn't exist
    try:
        # Create role with trust policy
        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "ec2.amazonaws.com"},
                    "Action": "sts:AssumeRole"
                }
            ]
        }

        iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy)
        )

        # Attach policies for AWS services access
        # S3 access
        iam.attach_role_policy(
            RoleName=role_name,
            PolicyArn='arn:aws:iam::aws:policy/AmazonS3FullAccess'
        )

        # SQS access
        iam.attach_role_policy(
            RoleName=role_name,
            PolicyArn='arn:aws:iam::aws:policy/AmazonSQSFullAccess'
        )

        # DynamoDB access
        iam.attach_role_policy(
            RoleName=role_name,
            PolicyArn='arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess'
        )

        # CloudWatch access for monitoring
        iam.attach_role_policy(
            RoleName=role_name,
            PolicyArn='arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy'
        )

        # Create custom policy for additional permissions
        custom_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents",
                        "logs:DescribeLogStreams"
                    ],
                    "Resource": "arn:aws:logs:*:*:*"
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "ec2:DescribeInstances",
                        "ec2:DescribeTags"
                    ],
                    "Resource": "*"
                }
            ]
        }

        try:
            # Create custom policy
            iam.create_policy(
                PolicyName='CrawlerCustomPolicy',
                PolicyDocument=json.dumps(custom_policy),
                Description='Custom policy for crawler nodes'
            )

            # Get the ARN of the newly created policy
            response = iam.list_policies(Scope='Local', PathPrefix='/')
            policy_arn = None
            for policy in response['Policies']:
                if policy['PolicyName'] == 'CrawlerCustomPolicy':
                    policy_arn = policy['Arn']
                    break

            if policy_arn:
                # Attach custom policy
                iam.attach_role_policy(
                    RoleName=role_name,
                    PolicyArn=policy_arn
                )
        except Exception as e:
            if 'EntityAlreadyExists' not in str(e):
                logging.error(f"Error creating custom policy: {e}")
            else:
                # Policy already exists, find and attach it
                response = iam.list_policies(Scope='Local', PathPrefix='/')
                for policy in response['Policies']:
                    if policy['PolicyName'] == 'CrawlerCustomPolicy':
                        iam.attach_role_policy(
                            RoleName=role_name,
                            PolicyArn=policy['Arn']
                        )
                        break

        logging.info(f"Created role with all necessary permissions: {role_name}")
    except Exception as e:
        if 'EntityAlreadyExists' not in str(e):
            logging.error(f"Error creating role: {e}")
        else:
            logging.info(f"Using existing role: {role_name}")

            # Ensure all policies are attached to existing role
            try:
                # Attach standard AWS policies
                policies = [
                    'arn:aws:iam::aws:policy/AmazonS3FullAccess',
                    'arn:aws:iam::aws:policy/AmazonSQSFullAccess',
                    'arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess',
                    'arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy'
                ]

                for policy_arn in policies:
                    try:
                        iam.attach_role_policy(
                            RoleName=role_name,
                            PolicyArn=policy_arn
                        )
                    except Exception:
                        # Policy might already be attached
                        pass

                # Check for custom policy
                response = iam.list_policies(Scope='Local', PathPrefix='/')
                for policy in response['Policies']:
                    if policy['PolicyName'] == 'CrawlerCustomPolicy':
                        try:
                            iam.attach_role_policy(
                                RoleName=role_name,
                                PolicyArn=policy['Arn']
                            )
                        except Exception:
                            # Policy might already be attached
                            pass
                        break
            except Exception as e2:
                logging.error(f"Error attaching policies to existing role: {e2}")

    # Create instance profile if it doesn't exist
    try:
        iam.create_instance_profile(InstanceProfileName=instance_profile_name)
        logging.info(f"Created instance profile: {instance_profile_name}")

        # Give AWS time to create the profile
        time.sleep(5)

        # Add role to profile
        iam.add_role_to_instance_profile(
            InstanceProfileName=instance_profile_name,
            RoleName=role_name
        )
    except Exception as e:
        if 'EntityAlreadyExists' not in str(e):
            logging.error(f"Error creating instance profile: {e}")

        logging.info(f"Using existing instance profile: {instance_profile_name}")

        # Check if role is attached to profile
        try:
            response = iam.get_instance_profile(InstanceProfileName=instance_profile_name)
            roles = response['InstanceProfile']['Roles']
            role_attached = False

            for role in roles:
                if role['RoleName'] == role_name:
                    role_attached = True
                    break

            if not role_attached:
                # Add role to profile if not already attached
                iam.add_role_to_instance_profile(
                    InstanceProfileName=instance_profile_name,
                    RoleName=role_name
                )
        except Exception as e2:
            logging.error(f"Error checking/updating instance profile: {e2}")

    return instance_profile_name

def find_latest_ami(ec2):
    """Find the latest Ubuntu 20.04 LTS AMI"""
    try:
        # Get the latest Ubuntu 20.04 LTS AMI (free tier eligible)
        response = ec2.describe_images(
            Owners=['099720109477'],  # Canonical's owner ID
            Filters=[
                {'Name': 'name', 'Values': ['ubuntu/images/hvm-ssd/ubuntu-focal-20.04-amd64-server-*']},
                {'Name': 'state', 'Values': ['available']},
                {'Name': 'virtualization-type', 'Values': ['hvm']}
            ]
        )

        # Sort by creation date
        images = sorted(response['Images'], key=lambda x: x['CreationDate'], reverse=True)
        ami_id = images[0]['ImageId']
        ami_name = images[0]['Name']
        logging.info(f"Using Ubuntu AMI: {ami_id} ({ami_name})")
        return ami_id
    except Exception as e:
        logging.error(f"Error finding Ubuntu AMI: {e}")
        # Fallback to a known Ubuntu AMI ID for us-east-1
        ami_id = 'ami-0557a15b87f6559cf'  # Ubuntu 20.04 LTS in us-east-1
        logging.info(f"Falling back to default Ubuntu AMI: {ami_id}")
        return ami_id

def create_user_data(bucket_name, sqs_queue, status_queue, region):
    """Create user data script for EC2 instance initialization"""
    user_data = f"""#!/bin/bash -xe
# Update system
apt-get update
apt-get upgrade -y
apt-get install -y python3-pip python3-dev build-essential git

# Install dependencies with compatible versions
pip3 install boto3==1.24.91 requests==2.28.1 beautifulsoup4==4.11.1 urllib3==1.26.15 whoosh==2.7.4

# Create working directory and storage directories
mkdir -p /home/ubuntu/crawler/logs
mkdir -p /home/ubuntu/crawler/local_storage/content
mkdir -p /home/ubuntu/crawler/search_index
cd /home/ubuntu/crawler
chmod 777 -R /home/ubuntu/crawler

# Download code from S3
echo "Downloading code from S3..."
wget "https://{bucket_name}.s3.amazonaws.com/code/main.py"
wget "https://{bucket_name}.s3.amazonaws.com/code/crawlerNode.py"
wget "https://{bucket_name}.s3.amazonaws.com/code/masterNode.py"
wget "https://{bucket_name}.s3.amazonaws.com/code/indexerNode.py"
wget "https://{bucket_name}.s3.amazonaws.com/code/cloud_queue.py"
wget "https://{bucket_name}.s3.amazonaws.com/code/cloud_storage.py"
wget "https://{bucket_name}.s3.amazonaws.com/code/db_manager.py"
wget "https://{bucket_name}.s3.amazonaws.com/code/requirements.txt"
wget "https://{bucket_name}.s3.amazonaws.com/code/update_timestamp.txt"

# Install any additional requirements
pip3 install -r requirements.txt

# Create a starter script with proper region
cat > /home/ubuntu/crawler/start_crawler.sh << SCRIPT
#!/bin/bash
cd /home/ubuntu/crawler
export AWS_DEFAULT_REGION={region}
python3 -u main.py --role crawler --sqs-queue {sqs_queue} --status-queue {status_queue} --bucket {bucket_name} --region {region} > logs/crawler.log 2>&1
SCRIPT

chmod +x /home/ubuntu/crawler/start_crawler.sh

# Set permissions
chown -R ubuntu:ubuntu /home/ubuntu/crawler
chmod 755 /home/ubuntu/crawler/start_crawler.sh

# Create a health check script
cat > /home/ubuntu/crawler/health_check.sh << 'SCRIPT'
#!/bin/bash
# Check if crawler is running
if ! pgrep -f "python3 -u main.py --role crawler" > /dev/null; then
    echo "Crawler not running. Restarting..."
    /home/ubuntu/crawler/start_crawler.sh
    # Log the restart
    echo "$(date): Crawler restarted" >> /home/ubuntu/crawler/logs/health_check.log
fi

# Check for common errors in logs
if grep -q "no attribute 'mark_url_as_fetched'" /home/ubuntu/crawler/logs/crawler.log; then
    echo "Detected missing method error. Attempting to fix by re-downloading db_manager.py..."
    cd /home/ubuntu/crawler
    wget -O db_manager.py "https://${bucket_name}.s3.amazonaws.com/code/db_manager.py"
    # Restart the crawler
    pkill -f "python3 -u main.py --role crawler"
    /home/ubuntu/crawler/start_crawler.sh
    echo "$(date): Fixed missing method error and restarted crawler" >> /home/ubuntu/crawler/logs/health_check.log
fi

# Check for updates by comparing timestamp
cd /home/ubuntu/crawler
current_timestamp=$(cat update_timestamp.txt 2>/dev/null | grep "Update timestamp" | cut -d' ' -f3 || echo "0")
echo "Current timestamp: $current_timestamp"

# Download the latest timestamp file
wget -q -O new_timestamp.txt "https://${bucket_name}.s3.amazonaws.com/code/update_timestamp.txt"
if [ $? -eq 0 ]; then
    new_timestamp=$(cat new_timestamp.txt | grep "Update timestamp" | cut -d' ' -f3 || echo "0")
    echo "New timestamp: $new_timestamp"

    # Compare timestamps
    if [ "$new_timestamp" != "$current_timestamp" ] && [ "$new_timestamp" != "0" ]; then
        echo "Update available. Downloading latest code..."

        # Download all code files
        wget -O main.py "https://${bucket_name}.s3.amazonaws.com/code/main.py"
        wget -O crawlerNode.py "https://${bucket_name}.s3.amazonaws.com/code/crawlerNode.py"
        wget -O masterNode.py "https://${bucket_name}.s3.amazonaws.com/code/masterNode.py"
        wget -O indexerNode.py "https://${bucket_name}.s3.amazonaws.com/code/indexerNode.py"
        wget -O cloud_queue.py "https://${bucket_name}.s3.amazonaws.com/code/cloud_queue.py"
        wget -O cloud_storage.py "https://${bucket_name}.s3.amazonaws.com/code/cloud_storage.py"
        wget -O db_manager.py "https://${bucket_name}.s3.amazonaws.com/code/db_manager.py"
        wget -O requirements.txt "https://${bucket_name}.s3.amazonaws.com/code/requirements.txt"
        wget -O update_timestamp.txt "https://${bucket_name}.s3.amazonaws.com/code/update_timestamp.txt"

        # Install any new requirements
        pip3 install -r requirements.txt

        # Restart the crawler
        echo "Restarting crawler with updated code..."
        pkill -f "python3 -u main.py --role crawler"
        /home/ubuntu/crawler/start_crawler.sh
        echo "$(date): Updated code to timestamp $new_timestamp and restarted crawler" >> /home/ubuntu/crawler/logs/health_check.log
    else
        echo "No updates available."
        rm new_timestamp.txt
    fi
else
    echo "Failed to check for updates."
fi
SCRIPT

chmod +x /home/ubuntu/crawler/health_check.sh

# Verify downloaded files
echo "Verifying downloaded files..."
for file in main.py crawlerNode.py masterNode.py indexerNode.py cloud_queue.py cloud_storage.py db_manager.py requirements.txt update_timestamp.txt; do
    if [ ! -f "$file" ]; then
        echo "ERROR: Failed to download $file, attempting to download again..."
        wget "https://${bucket_name}.s3.amazonaws.com/code/$file"
    fi
done

# Log the initial timestamp
initial_timestamp=$(cat update_timestamp.txt 2>/dev/null | grep "Update timestamp" | cut -d' ' -f3 || echo "0")
echo "Initial code timestamp: $initial_timestamp" >> /home/ubuntu/crawler/logs/health_check.log

# Start the crawler process
sudo -u ubuntu /home/ubuntu/crawler/start_crawler.sh &

# Set up cron job for health check only
echo "*/2 * * * * /home/ubuntu/crawler/health_check.sh" > /tmp/crawler_cron
crontab -u ubuntu /tmp/crawler_cron

# Set up basic monitoring
echo "Setting up basic monitoring..."

# Install CloudWatch agent
wget https://s3.amazonaws.com/amazoncloudwatch-agent/ubuntu/amd64/latest/amazon-cloudwatch-agent.deb
dpkg -i amazon-cloudwatch-agent.deb

echo "Crawler node setup complete!"
"""
    return user_data

def launch_auto_scaling_group(key_name, instance_type, min_size, max_size, sqs_queue, status_queue, bucket_name, region='us-east-1'):
    """Create and launch an auto scaling group for crawler nodes"""
    # Initialize AWS clients
    ec2 = boto3.client('ec2', region_name=region)
    iam = boto3.client('iam', region_name=region)
    autoscaling = boto3.client('autoscaling', region_name=region)

    # Upload code to S3
    upload_latest_code(bucket_name, region)

    # Create security group
    sg_id = create_security_group(ec2)
    if not sg_id:
        return False

    # Setup IAM role
    instance_profile_name = setup_iam_role(iam)

    # Find latest AMI
    ami_id = find_latest_ami(ec2)

    # Create user data script with proper region
    user_data = create_user_data(bucket_name, sqs_queue, status_queue, region)
    user_data_encoded = base64.b64encode(user_data.encode()).decode()

    # Create launch template
    launch_template_name = 'crawler-launch-template'
    try:
        # Try to create a new launch template
        logging.info(f"Creating launch template: {launch_template_name}")
        response = ec2.create_launch_template(
            LaunchTemplateName=launch_template_name,
            VersionDescription='Initial version',
            LaunchTemplateData={
                'ImageId': ami_id,
                'InstanceType': instance_type,
                'KeyName': key_name,
                'UserData': user_data_encoded,
                'SecurityGroupIds': [sg_id],
                'IamInstanceProfile': {
                    'Name': instance_profile_name
                },
                'TagSpecifications': [
                    {
                        'ResourceType': 'instance',
                        'Tags': [
                            {'Key': 'Name', 'Value': 'CrawlerNode'},
                            {'Key': 'Role', 'Value': 'crawler'}
                        ]
                    }
                ]
            }
        )
        launch_template_id = response['LaunchTemplate']['LaunchTemplateId']
        logging.info(f"Created launch template: {launch_template_id}")
    except Exception as e:
        if 'AlreadyExists' in str(e):
            # Get the existing template ID
            response = ec2.describe_launch_templates(
                LaunchTemplateNames=[launch_template_name]
            )
            launch_template_id = response['LaunchTemplates'][0]['LaunchTemplateId']
            logging.info(f"Using existing launch template: {launch_template_id}")

            # Create a new version of the template
            response = ec2.create_launch_template_version(
                LaunchTemplateId=launch_template_id,
                VersionDescription='Updated version',
                LaunchTemplateData={
                    'ImageId': ami_id,
                    'InstanceType': instance_type,
                    'KeyName': key_name,
                    'UserData': user_data_encoded,
                    'SecurityGroupIds': [sg_id],
                    'IamInstanceProfile': {
                        'Name': instance_profile_name
                    },
                    'TagSpecifications': [
                        {
                            'ResourceType': 'instance',
                            'Tags': [
                                {'Key': 'Name', 'Value': 'CrawlerNode'},
                                {'Key': 'Role', 'Value': 'crawler'}
                            ]
                        }
                    ]
                }
            )
            logging.info(f"Created new launch template version: {response['LaunchTemplateVersion']['VersionNumber']}")
        else:
            logging.error(f"Error creating launch template: {e}")
            return False

    # Check if Auto Scaling group already exists
    asg_name = 'crawler-auto-scaling-group'
    try:
        autoscaling.describe_auto_scaling_groups(
            AutoScalingGroupNames=[asg_name]
        )
        # If it exists, update it
        logging.info(f"Updating existing Auto Scaling group: {asg_name}")
        autoscaling.update_auto_scaling_group(
            AutoScalingGroupName=asg_name,
            LaunchTemplate={
                'LaunchTemplateId': launch_template_id,
                'Version': '$Latest'
            },
            MinSize=min_size,
            MaxSize=max_size,
            DesiredCapacity=min_size
        )
    except Exception as e:
        if 'not found' in str(e).lower():
            # Create Auto Scaling group
            logging.info(f"Creating Auto Scaling group: {asg_name}")

            # Hardcoded availability zones based on region
            availability_zones = []
            if region == 'us-east-1':
                availability_zones = ['us-east-1a', 'us-east-1b']
            else:
                # For other regions, get the first 2 AZs
                try:
                    az_response = ec2.describe_availability_zones(
                        Filters=[{'Name': 'region-name', 'Values': [region]}]
                    )
                    availability_zones = [az['ZoneName'] for az in az_response['AvailabilityZones'][:2]]
                except Exception as az_error:
                    logging.error(f"Error getting availability zones: {az_error}")
                    return False

            if not availability_zones:
                logging.error("No availability zones found")
                return False

            logging.info(f"Using availability zones: {availability_zones}")

            try:
                autoscaling.create_auto_scaling_group(
                    AutoScalingGroupName=asg_name,
                    LaunchTemplate={
                        'LaunchTemplateId': launch_template_id,
                        'Version': '$Latest'
                    },
                    MinSize=min_size,
                    MaxSize=max_size,
                    DesiredCapacity=min_size,
                    AvailabilityZones=availability_zones,
                    Tags=[
                        {
                            'Key': 'Name',
                            'Value': 'CrawlerNode',
                            'PropagateAtLaunch': True
                        },
                        {
                            'Key': 'Role',
                            'Value': 'crawler',
                            'PropagateAtLaunch': True
                        }
                    ]
                )
                logging.info(f"Created Auto Scaling group: {asg_name}")
            except Exception as create_asg_error:
                logging.error(f"Error creating Auto Scaling group: {create_asg_error}")
                return False
        else:
            logging.error(f"Error checking Auto Scaling group: {e}")
            return False

    # Create scaling policies (CPU based)
    try:
        logging.info("Creating scaling policies...")
        # Scale out policy
        autoscaling.put_scaling_policy(
            AutoScalingGroupName=asg_name,
            PolicyName='crawler-scale-out',
            PolicyType='TargetTrackingScaling',
            TargetTrackingConfiguration={
                'PredefinedMetricSpecification': {
                    'PredefinedMetricType': 'ASGAverageCPUUtilization'
                },
                'TargetValue': 2.0,  # Scale out when CPU exceeds 40% (lowered from 70%)
                'ScaleOutCooldown': 10,  # 2 minutes (reduced from 5 minutes)
                'ScaleInCooldown': 300  # 5 minutes
            }
        )

        # Set a fixed minimum instance count regardless of CPU
        min_size_param = min_size
        if min_size < 2:
            logging.info(f"Overriding minimum instance count to 2 (was {min_size})")
            min_size_param = 2

        autoscaling.update_auto_scaling_group(
            AutoScalingGroupName=asg_name,
            MinSize=min_size_param,
            DesiredCapacity=min_size_param
        )

        logging.info("Created scaling policies and updated minimum instance count")
    except Exception as e:
        logging.error(f"Error creating scaling policies: {e}")
        # Continue anyway, scaling group should still work

    logging.info(f"Auto Scaling group setup complete: {asg_name}")
    logging.info(f"Min instances: {min_size}, Max instances: {max_size}")

    return True

def main():
    parser = argparse.ArgumentParser(description='Deploy auto-scaling crawler nodes in AWS')
    parser.add_argument('--key-name', required=True,
                        help='EC2 key pair name for SSH access')
    parser.add_argument('--instance-type', default='t2.micro',
                        help='EC2 instance type for crawler nodes')
    parser.add_argument('--min-size', type=int, default=2,
                        help='Minimum number of instances in auto scaling group')
    parser.add_argument('--max-size', type=int, default=5,
                        help='Maximum number of instances in auto scaling group')
    parser.add_argument('--sqs-queue', default='crawler-url-queue',
                        help='Name of the SQS queue for URL tasks')
    parser.add_argument('--status-queue', default='crawler-status-queue',
                        help='Name of the SQS queue for status updates')
    parser.add_argument('--bucket', default='web-crawler-data-storage',
                        help='Name of the S3 bucket for code storage')
    parser.add_argument('--region', default='us-east-1',
                        help='AWS region for deployment')

    args = parser.parse_args()

    # Ensure at least 2 instances for proper auto-scaling
    if args.min_size < 2:
        print("Warning: Minimum instances should be at least 2 for proper auto-scaling")
        print("Setting minimum instances to 2")
        args.min_size = 2

    success = launch_auto_scaling_group(
        args.key_name,
        args.instance_type,
        args.min_size,
        args.max_size,
        args.sqs_queue,
        args.status_queue,
        args.bucket,
        args.region
    )

    if success:
        print("\nCrawler auto scaling group deployed successfully!")
        print(f"Auto Scaling Group: crawler-auto-scaling-group")
        print(f"Min instances: {args.min_size}, Max instances: {args.max_size}")
        print(f"Instance type: {args.instance_type}")
        print(f"\nTo check status:")
        print(f"aws autoscaling describe-auto-scaling-groups --auto-scaling-group-names crawler-auto-scaling-group --region {args.region}")
        return 0
    else:
        print("\nFailed to deploy crawler auto scaling group.")
        return 1

if __name__ == "__main__":
    exit(main())