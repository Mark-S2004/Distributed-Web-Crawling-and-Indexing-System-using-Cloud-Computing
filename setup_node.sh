#!/bin/bash
# Script to set up a node for the distributed web crawler system

# Detect and display node information
echo "Setting up web crawler node on $(hostname)"
echo "IP address: $(hostname -I)"

# Update system packages
echo "Updating system packages..."
sudo apt-get update -y
sudo apt-get install -y python3-pip

# Install Python packages
echo "Installing required Python packages..."
pip3 install nltk boto3 requests bs4 whoosh

# Download NLTK data
echo "Downloading NLTK data..."
python3 -c "import nltk; nltk.download('punkt'); nltk.download('wordnet'); nltk.download('stopwords')"

# Ensure required directories exist
echo "Creating required directories..."
mkdir -p logs
mkdir -p data/monitoring
mkdir -p data/queue
mkdir -p search_index

# Check AWS configuration
if [ -f ~/.aws/credentials ] || [ -f ~/.aws/config ]; then
    echo "AWS configuration found."
else
    echo "AWS configuration not found. You may need to run 'aws configure'."
    echo "For this test, local fallback mode will be used if AWS credentials are not configured."
fi

echo "Node setup complete. You can now run the crawler with one of these commands:"
echo "  - Master node:   python3 main.py --role master --sqs-queue crawler-url-queue --status-queue crawler-status-queue --bucket web-crawler-data-storage"
echo "  - Crawler node:  python3 main.py --role crawler --sqs-queue crawler-url-queue --status-queue crawler-status-queue --bucket web-crawler-data-storage"
echo "  - Indexer node:  python3 main.py --role indexer --sqs-queue crawler-url-queue --status-queue crawler-status-queue --bucket web-crawler-data-storage" 