#!/bin/bash
# AWS EC2 Bootstrap Script for Distributed Web Crawler
# This script prepares an EC2 instance to participate in the distributed crawler cluster

# Exit on any error
set -e

# Get instance metadata
INSTANCE_ID=$(curl -s http://169.254.169.254/latest/meta-data/instance-id)
HOSTNAME=$(curl -s http://169.254.169.254/latest/meta-data/hostname)
PRIVATE_IP=$(curl -s http://169.254.169.254/latest/meta-data/local-ipv4)

# Fetch instance tags to determine role
echo "Fetching instance tags to determine role..."
REGION=$(curl -s http://169.254.169.254/latest/meta-data/placement/region)
ROLE=$(aws ec2 describe-tags --region $REGION --filters "Name=resource-id,Values=$INSTANCE_ID" "Name=key,Values=Role" --query "Tags[0].Value" --output text)

# If role tag is missing, default to crawler
if [ "$ROLE" == "None" ]; then
    ROLE="crawler"
fi

echo "Instance $INSTANCE_ID ($HOSTNAME) has role: $ROLE"

# Create working directory
WORK_DIR="/home/ec2-user/crawler"
mkdir -p $WORK_DIR
cd $WORK_DIR

# Install dependencies
echo "Installing dependencies..."
sudo yum update -y
sudo yum install -y python3 python3-pip git gcc python3-devel
sudo yum install -y openmpi openmpi-devel

# Install Python packages
echo "Installing Python packages..."
pip3 install --user mpi4py boto3 requests beautifulsoup4 whoosh nltk

# Download NLTK data
echo "Downloading NLTK data..."
python3 -c "import nltk; nltk.download('punkt'); nltk.download('stopwords'); nltk.download('wordnet')"

# Clone code repository (or pull latest changes if it exists)
echo "Setting up code repository..."
REPO_URL="https://github.com/yourusername/distributed-web-crawler.git"

if [ -d "$WORK_DIR/repo" ]; then
    cd $WORK_DIR/repo
    git pull
else
    git clone $REPO_URL $WORK_DIR/repo
    cd $WORK_DIR/repo
fi

# Create data directories
mkdir -p data/logs
mkdir -p data/metrics
mkdir -p data/db
mkdir -p data/queue
mkdir -p data/raw_html
mkdir -p data/processed_text
mkdir -p data/search_index

# Generate machinefile for MPI
echo "Finding cluster nodes and generating machinefile..."
MACHINEFILE="$WORK_DIR/machinefile"

# Find all instances that are part of this crawler cluster using the same security group
SG_ID=$(aws ec2 describe-instances --region $REGION --instance-ids $INSTANCE_ID --query "Reservations[0].Instances[0].SecurityGroups[0].GroupId" --output text)

# Get all running instances with the same security group
INSTANCES=$(aws ec2 describe-instances --region $REGION --filters "Name=instance.group-id,Values=$SG_ID" "Name=instance-state-name,Values=running" --query "Reservations[].Instances[].[InstanceId,PrivateIpAddress,Tags[?Key=='Role'].Value|[0]]" --output text)

# Build the hosts file entries and machinefile for MPI
echo "# Crawler cluster hosts" > /tmp/crawler_hosts
echo > $MACHINEFILE

# Parse the instance data and create the files
while read -r instance_id ip role; do
    # Skip instances without an IP
    if [ -z "$ip" ]; then
        continue
    fi
    
    # Set a default role if none specified
    if [ -z "$role" ]; then
        role="crawler"
    fi
    
    # Add to hosts file with role as hostname
    echo "$ip ${role}-${instance_id}" >> /tmp/crawler_hosts
    
    # Add to machinefile for MPI (only add master and crawler nodes)
    if [ "$role" == "master" ] || [ "$role" == "crawler" ]; then
        echo "$ip" >> $MACHINEFILE
    fi
done <<< "$INSTANCES"

# Update the hosts file
sudo cat /tmp/crawler_hosts >> /etc/hosts
rm /tmp/crawler_hosts

# Set up SSH for passwordless connections (required for MPI)
echo "Setting up SSH for passwordless connections..."
if [ ! -f ~/.ssh/id_rsa ]; then
    ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa
fi

# Upload public key to S3 bucket
S3_BUCKET="crawler-config-bucket"
aws s3 cp ~/.ssh/id_rsa.pub s3://$S3_BUCKET/ssh-keys/$INSTANCE_ID.pub

# Download and add all public keys from other nodes
mkdir -p ~/.ssh/tmp
aws s3 cp s3://$S3_BUCKET/ssh-keys/ ~/.ssh/tmp/ --recursive
cat ~/.ssh/tmp/*.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
rm -rf ~/.ssh/tmp

# Disable host key checking for cluster IPs
cat > ~/.ssh/config <<EOF
Host 10.*
    StrictHostKeyChecking no
    UserKnownHostsFile=/dev/null
EOF
chmod 600 ~/.ssh/config

# Configure environment variables
echo "export PATH=$PATH:/usr/lib64/openmpi/bin" >> ~/.bashrc
echo "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/lib64/openmpi/lib" >> ~/.bashrc
source ~/.bashrc

# Configure AWS region for boto3
mkdir -p ~/.aws
cat > ~/.aws/config <<EOF
[default]
region = $REGION
EOF

# Start appropriate process based on role
echo "Starting process for role: $ROLE"

if [ "$ROLE" == "master" ]; then
    # Start master process
    echo "Starting master node process..."
    cd $WORK_DIR/repo
    
    # Wait for other nodes to come online
    sleep 30
    
    # Count nodes in machinefile to determine MPI processes
    NODE_COUNT=$(wc -l < $MACHINEFILE)
    echo "Detected $NODE_COUNT nodes in the cluster"
    
    # Generate seed URLs if none exist
    if [ ! -f "$WORK_DIR/repo/data/seed_urls.txt" ]; then
        echo "https://www.python.org" > $WORK_DIR/repo/data/seed_urls.txt
        echo "https://www.github.com" >> $WORK_DIR/repo/data/seed_urls.txt
        echo "https://www.wikipedia.org" >> $WORK_DIR/repo/data/seed_urls.txt
        echo "https://www.reddit.com" >> $WORK_DIR/repo/data/seed_urls.txt
        echo "https://www.stackoverflow.com" >> $WORK_DIR/repo/data/seed_urls.txt
    fi
    
    # Run the MPI job
    mpirun -n $NODE_COUNT -machinefile $MACHINEFILE python3 main.py &> $WORK_DIR/crawler.log &
    
    echo "Master node started! Check $WORK_DIR/crawler.log for output."
    
elif [ "$ROLE" == "indexer" ]; then
    # Indexer node - will be started by the master through MPI
    echo "Indexer node ready. Will be started by the master node."
    
else
    # Crawler node - will be started by the master through MPI
    echo "Crawler node ready. Will be started by the master node."
fi

# Set up CloudWatch agent for monitoring
echo "Setting up CloudWatch monitoring..."
sudo yum install -y amazon-cloudwatch-agent

# Create CloudWatch agent configuration
cat > /tmp/cloudwatch-config.json <<EOF
{
  "agent": {
    "metrics_collection_interval": 60,
    "run_as_user": "ec2-user"
  },
  "metrics": {
    "namespace": "CrawlerMetrics",
    "metrics_collected": {
      "mem": {
        "measurement": ["mem_used_percent"]
      },
      "disk": {
        "measurement": ["disk_used_percent"],
        "resources": ["/"]
      },
      "cpu": {
        "resources": ["*"],
        "measurement": ["cpu_usage_idle", "cpu_usage_user", "cpu_usage_system"]
      }
    },
    "append_dimensions": {
      "InstanceId": "${aws:InstanceId}",
      "InstanceType": "${aws:InstanceType}",
      "Role": "$ROLE"
    }
  },
  "logs": {
    "logs_collected": {
      "files": {
        "collect_list": [
          {
            "file_path": "$WORK_DIR/crawler.log",
            "log_group_name": "/crawler/$ROLE",
            "log_stream_name": "{instance_id}",
            "retention_in_days": 7
          },
          {
            "file_path": "$WORK_DIR/repo/data/logs/*.log",
            "log_group_name": "/crawler/nodes",
            "log_stream_name": "{instance_id}-{file_name}",
            "retention_in_days": 7
          }
        ]
      }
    }
  }
}
EOF

# Start the CloudWatch agent
sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a fetch-config -m ec2 -s -c file:/tmp/cloudwatch-config.json

echo "Bootstrap complete! Instance is ready for crawler operations." 