#!/usr/bin/env python3
import os
import logging
import argparse
import traceback
import boto3
from flask import Flask, request, render_template, jsonify
from indexerNode import WhooshIndexer
from cloud_queue import CloudQueue
from db_manager import DBManager
from urllib.parse import urlparse

# Set up logging
os.makedirs("logs", exist_ok=True)
log_file = os.path.join("logs", "web_search.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - WebSearch - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

# Initialize Flask app
app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False  # For proper UTF-8 handling
app.secret_key = os.urandom(24)  # For flash messages

@app.after_request
def add_header(response):
    """Add headers to improve security and performance."""
    # Cache control - Set to no-cache for auto-refresh
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'

    # Security headers
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'SAMEORIGIN'
    response.headers['X-XSS-Protection'] = '1; mode=block'
    response.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains'

    # Content Security Policy
    response.headers['Content-Security-Policy'] = "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline';"

    # Simplify server header
    response.headers['Server'] = 'WebCrawler'

    return response

# Initialize AWS services
aws_services_available = False
task_queue = None
db_manager = None
s3_client = None
logs_client = None
ec2_client = None
sqs_client = None
dynamodb_client = None

try:
    # Initialize AWS clients
    s3_client = boto3.client('s3')
    logs_client = boto3.client('logs')
    ec2_client = boto3.client('ec2')
    sqs_client = boto3.client('sqs')
    dynamodb_client = boto3.client('dynamodb')

    # Initialize higher-level services
    task_queue = CloudQueue(queue_name='crawler-url-queue', status_queue_name='crawler-status-queue')
    db_manager = DBManager()

    # If we got here, all services are available
    aws_services_available = True
    logging.info("AWS services initialized successfully")
except Exception as e:
    logging.error(f"Failed to initialize AWS services: {e}")
    logging.debug(traceback.format_exc())
    logging.warning("Some features requiring AWS services will be unavailable")

# Create templates directory if it doesn't exist
os.makedirs("templates", exist_ok=True)

# Create a simple HTML template for the search interface
with open("templates/search.html", "w") as f:
    f.write("""
<!DOCTYPE html>
<html lang="en">
<head>
    <title>Web Crawler Search</title>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="refresh" content="10">
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background-color: white;
            padding: 20px;
            border-radius: 5px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
        }
        h1, h2, h3 {
            color: #333;
        }
        .search-form {
            margin: 20px 0;
        }
        .search-input {
            width: 70%;
            padding: 10px;
            font-size: 16px;
            border: 1px solid #ddd;
            border-radius: 4px;
        }
        .search-button, .seed-button {
            padding: 10px 20px;
            font-size: 16px;
            background-color: #4285f4;
            color: white;
            border: none;
            border-radius: 4px;
            cursor: pointer;
        }
        .search-button:hover, .seed-button:hover {
            background-color: #3367d6;
        }
        .result {
            margin: 20px 0;
            padding: 15px;
            border: 1px solid #ddd;
            border-radius: 4px;
        }
        .result h2 {
            margin-top: 0;
            color: #1a0dab;
        }
        .result a {
            color: #1a0dab;
            text-decoration: none;
        }
        .result a:hover {
            text-decoration: underline;
        }
        .result .url {
            color: #006621;
            font-size: 14px;
            margin-bottom: 5px;
        }
        .result .summary {
            color: #545454;
            font-size: 14px;
        }
        .result .keywords {
            color: #777;
            font-size: 12px;
            margin-top: 5px;
        }
        .no-results {
            margin: 20px 0;
            color: #777;
        }
        .error {
            color: red;
            margin: 20px 0;
            padding: 10px;
            background-color: #ffeeee;
            border: 1px solid #ffcccc;
            border-radius: 4px;
        }
        .success {
            color: green;
            margin: 20px 0;
            padding: 10px;
            background-color: #eeffee;
            border: 1px solid #ccffcc;
            border-radius: 4px;
        }
        .info-message {
            color: #0066cc;
            margin: 20px 0;
            padding: 10px;
            background-color: #eeeeff;
            border: 1px solid #ccccff;
            border-radius: 4px;
        }
        .section {
            margin: 30px 0;
            padding: 20px;
            border: 1px solid #ddd;
            border-radius: 5px;
            background-color: #f9f9f9;
        }
        .section h2 {
            margin-top: 0;
            padding-bottom: 10px;
            border-bottom: 2px solid #4285f4;
            color: #333;
        }
        .subsection {
            margin: 20px 0;
            padding: 15px;
            border: 1px solid #ddd;
            border-radius: 4px;
            background-color: white;
        }
        .subsection h3 {
            margin-top: 0;
            color: #4285f4;
        }
        .seed-form {
            margin: 20px 0;
            padding: 15px;
            border: 1px solid #ddd;
            border-radius: 4px;
            background-color: #f9f9f9;
        }
        .seed-input {
            width: 70%;
            padding: 10px;
            font-size: 16px;
            border: 1px solid #ddd;
            border-radius: 4px;
        }
        .log-container {
            max-height: 400px;
            overflow-y: auto;
            background-color: #f5f5f5;
            padding: 10px;
            border-radius: 4px;
            font-family: monospace;
            font-size: 14px;
        }
        .log-entry {
            margin: 5px 0;
            padding: 5px;
            border-bottom: 1px solid #ddd;
        }
        .metrics-card {
            margin: 10px 0;
            padding: 15px;
            border: 1px solid #ddd;
            border-radius: 4px;
            background-color: #f9f9f9;
        }
        .metrics-title {
            font-weight: bold;
            margin-bottom: 10px;
        }
        .metrics-value {
            font-size: 24px;
            color: #4285f4;
        }
        .flex-container {
            display: flex;
            flex-wrap: wrap;
            gap: 20px;
        }
        .flex-item {
            flex: 1;
            min-width: 300px;
        }
        .action-button {
            padding: 8px 16px;
            font-size: 14px;
            background-color: #4285f4;
            color: white;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            margin: 5px 0;
        }
        .action-button:hover {
            background-color: #3367d6;
        }
        .action-button.danger {
            background-color: #dc3545;
        }
        .action-button.danger:hover {
            background-color: #bd2130;
        }
        .instance-card {
            margin: 10px 0;
            padding: 15px;
            border: 1px solid #ddd;
            border-radius: 4px;
            background-color: #f9f9f9;
        }
        .instance-actions {
            margin-top: 10px;
        }
        .table {
            width: 100%;
            border-collapse: collapse;
            margin: 15px 0;
        }
        .table th, .table td {
            padding: 8px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }
        .table th {
            background-color: #f2f2f2;
        }
        .confirmation-dialog {
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background-color: rgba(0, 0, 0, 0.5);
            display: flex;
            justify-content: center;
            align-items: center;
            z-index: 1000;
        }
        .confirmation-content {
            background-color: white;
            padding: 20px;
            border-radius: 5px;
            max-width: 500px;
            width: 100%;
        }
        .confirmation-buttons {
            display: flex;
            justify-content: flex-end;
            margin-top: 20px;
        }
        .confirmation-buttons button {
            margin-left: 10px;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>Web Crawler Search & Monitoring</h1>

        <!-- Search Section -->
        <div class="section">
            <h2>Search</h2>
            <div class="search-form">
                <form action="/" method="get">
                    <input type="text" name="q" class="search-input" value="{{ query }}" placeholder="Enter search query...">
                    <button type="submit" class="search-button">Search</button>
                </form>
            </div>

            {% if error %}
            <div class="error">
                <p>{{ error }}</p>
            </div>
            {% endif %}

            {% if results %}
            <div class="results">
                <p>Found {{ results|length }} results:</p>

                {% for result in results %}
                <div class="result">
                    <h2><a href="{{ result.url }}" target="_blank">{{ result.title }}</a></h2>
                    <div class="url">{{ result.url }}</div>
                    {% if result.summary %}
                    <div class="summary">{{ result.summary }}</div>
                    {% endif %}
                    {% if result.keywords %}
                    <div class="keywords">Keywords: {{ result.keywords }}</div>
                    {% endif %}
                </div>
                {% endfor %}
            </div>
            {% elif query %}
            <div class="no-results">
                <p>No results found for "{{ query }}".</p>
            </div>
            {% endif %}
        </div>

        <!-- Seed URLs Section -->
        <div class="section">
            <h2>Seed URLs to Crawler</h2>
            <p>Add new URLs for the crawler to process:</p>

            <div class="seed-form">
                <input type="text" id="seed-url" class="seed-input" placeholder="Enter URL to seed (e.g., https://example.com)">
                <button onclick="seedUrl()" class="seed-button">Add URL</button>
            </div>

            <div id="seed-result"></div>
        </div>

        <!-- AWS Monitoring Section -->
        <div class="section">
            <h2>AWS Service Monitoring</h2>

            <!-- AWS Status Message -->
            <div id="aws-status-message"></div>

            <!-- SQS Section -->
            <div class="subsection">
                <h3>SQS Queue Metrics</h3>
                <div id="sqs-metrics" class="flex-container">
                    <div class="flex-item">
                        <div class="metrics-card">
                            <div class="metrics-title">URL Queue Messages</div>
                            <div class="metrics-value" id="url-queue-messages">Loading...</div>
                        </div>
                    </div>
                    <div class="flex-item">
                        <div class="metrics-card">
                            <div class="metrics-title">Status Queue Messages</div>
                            <div class="metrics-value" id="status-queue-messages">Loading...</div>
                        </div>
                    </div>
                </div>

                <h4>Sample Messages</h4>
                <div id="sqs-messages" class="log-container">Loading...</div>
            </div>

            <!-- DynamoDB Section -->
            <div class="subsection">
                <h3>DynamoDB Tables</h3>
                <div id="dynamodb-tables" class="flex-container">Loading...</div>
            </div>

            <!-- S3 Section -->
            <div class="subsection">
                <h3>S3 Bucket Metrics</h3>
                <div id="s3-metrics" class="flex-container">
                    <div class="flex-item">
                        <div class="metrics-card">
                            <div class="metrics-title">Object Count</div>
                            <div class="metrics-value" id="s3-object-count">Loading...</div>
                        </div>
                    </div>
                    <div class="flex-item">
                        <div class="metrics-card">
                            <div class="metrics-title">Total Size</div>
                            <div class="metrics-value" id="s3-total-size">Loading...</div>
                        </div>
                    </div>
                </div>

                <h4>Recent Objects</h4>
                <div id="s3-objects" class="log-container">Loading...</div>
            </div>

            <!-- EC2 Section -->
            <div class="subsection">
                <h3>EC2 Instances</h3>
                <div id="ec2-instances">Loading...</div>
            </div>

            <!-- Logs Section -->
            <div class="subsection">
                <h3>CloudWatch Logs</h3>
                <div id="cloudwatch-logs" class="log-container">Loading...</div>
            </div>
        </div>

        <!-- AWS Management Section -->
        <div class="section">
            <h2>AWS Resource Management</h2>
            <p>Manage AWS resources for the crawler system. <strong>Warning:</strong> These actions can affect the running system.</p>

            <!-- EC2 Management Section -->
            <div class="subsection">
                <h3>EC2 Instance Management</h3>
                <p>Start, stop, or restart EC2 instances.</p>

                <div id="ec2-management-list">Loading instances...</div>
            </div>

            <!-- SQS Management Section -->
            <div class="subsection">
                <h3>SQS Queue Management</h3>
                <p>Purge SQS queues to remove all messages.</p>

                <div class="metrics-card">
                    <div class="metrics-title">URL Queue</div>
                    <p>Purge all messages from the crawler URL queue.</p>
                    <button id="purge-url-queue-btn" class="action-button">Purge URL Queue</button>
                    <div id="purge-crawlerurlqueue-result"></div>
                </div>

                <div class="metrics-card">
                    <div class="metrics-title">Status Queue</div>
                    <p>Purge all messages from the crawler status queue.</p>
                    <button id="purge-status-queue-btn" class="action-button">Purge Status Queue</button>
                    <div id="purge-crawlerstatusqueue-result"></div>
                </div>
            </div>

            <!-- DynamoDB Management Section -->
            <div class="subsection">
                <h3>DynamoDB Table Management</h3>
                <p>Clear DynamoDB tables to remove all items.</p>

                <div id="dynamodb-management-list">Loading tables...</div>
            </div>

            <!-- S3 Management Section -->
            <div class="subsection">
                <h3>S3 Bucket Management</h3>
                <p>Empty S3 bucket to remove all objects.</p>

                <div class="metrics-card">
                    <div class="metrics-title">Web Crawler Data Storage</div>
                    <p>Empty the web crawler data storage bucket.</p>
                    <button id="empty-s3-bucket-btn" class="action-button">Empty Bucket</button>
                    <div id="empty-webcrawlerdatastorage-result"></div>
                </div>
            </div>

            <!-- Reset All Section -->
            <div class="subsection">
                <h3>Reset All Resources</h3>
                <p><strong>Warning:</strong> This will reset all AWS resources related to the crawler system. This action cannot be undone.</p>

                <div class="metrics-card">
                    <div class="metrics-title">Reset All Resources</div>
                    <p>Reset all SQS queues, DynamoDB tables, and S3 bucket.</p>
                    <button id="reset-all-resources-btn" class="action-button danger">Reset All Resources</button>
                    <div id="reset-all-result"></div>
                </div>
            </div>
        </div>
    </div>

    <script>
        // Add auto-refresh indicator
        document.addEventListener('DOMContentLoaded', function() {
            // Create auto-refresh indicator
            var body = document.body;
            var refreshIndicator = document.createElement('div');
            refreshIndicator.style.position = 'fixed';
            refreshIndicator.style.bottom = '10px';
            refreshIndicator.style.right = '10px';
            refreshIndicator.style.backgroundColor = '#4285f4';
            refreshIndicator.style.color = 'white';
            refreshIndicator.style.padding = '5px 10px';
            refreshIndicator.style.borderRadius = '4px';
            refreshIndicator.style.fontSize = '12px';
            refreshIndicator.style.zIndex = '1000';

            // Add countdown timer
            var countdown = 10;
            refreshIndicator.textContent = 'Auto-refresh in ' + countdown + 's';
            body.appendChild(refreshIndicator);

            // Update countdown timer
            var timer = setInterval(function() {
                countdown--;
                if (countdown <= 0) {
                    clearInterval(timer);
                    refreshIndicator.textContent = 'Refreshing...';
                } else {
                    refreshIndicator.textContent = 'Auto-refresh in ' + countdown + 's';
                }
            }, 1000);
        });

        // Initialize page when document is ready
        document.addEventListener('DOMContentLoaded', function() {
            console.log("Document ready");

            // Set initial loading messages with timeouts
            setupLoadingTimeouts();

            // Load AWS metrics
            loadAwsMetrics();

            // Load EC2 management data
            loadEc2Management();

            // Load DynamoDB management data
            loadDynamoDbManagement();

            // Add event listener for seed URL on Enter key
            var seedUrlInput = document.getElementById("seed-url");
            if (seedUrlInput) {
                seedUrlInput.addEventListener("keyup", function(event) {
                    if (event.key === "Enter") {
                        seedUrl();
                    }
                });
            }

            // Add event listeners for SQS queue purge buttons
            var purgeUrlQueueBtn = document.getElementById("purge-url-queue-btn");
            if (purgeUrlQueueBtn) {
                purgeUrlQueueBtn.addEventListener("click", function() {
                    purgeSqsQueue('crawler-url-queue');
                });
            }

            var purgeStatusQueueBtn = document.getElementById("purge-status-queue-btn");
            if (purgeStatusQueueBtn) {
                purgeStatusQueueBtn.addEventListener("click", function() {
                    purgeSqsQueue('crawler-status-queue');
                });
            }

            // Add event listener for S3 bucket emptying
            var emptyS3BucketBtn = document.getElementById("empty-s3-bucket-btn");
            if (emptyS3BucketBtn) {
                emptyS3BucketBtn.addEventListener("click", function() {
                    emptyS3Bucket('web-crawler-data-storage');
                });
            }

            // Add event listener for Reset All Resources button
            var resetAllResourcesBtn = document.getElementById("reset-all-resources-btn");
            if (resetAllResourcesBtn) {
                resetAllResourcesBtn.addEventListener("click", function() {
                    resetAllResources();
                });
            }
        });

        // Setup loading timeouts to show helpful messages if loading takes too long
        function setupLoadingTimeouts() {
            // List of elements that show loading messages
            const loadingElements = [
                "url-queue-messages",
                "status-queue-messages",
                "sqs-messages",
                "dynamodb-tables",
                "s3-object-count",
                "s3-total-size",
                "s3-objects",
                "ec2-instances",
                "cloudwatch-logs",
                "ec2-management-list",
                "dynamodb-management-list"
            ];

            // Set a timeout for each element
            loadingElements.forEach(elementId => {
                setTimeout(function() {
                    const element = document.getElementById(elementId);
                    if (element && element.textContent.includes("Loading")) {
                        element.innerHTML = '<div class="error">Unable to load data. AWS services may not be available or properly configured. Check your AWS credentials and network connection.</div>';
                    }
                }, 5000); // 5 second timeout
            });
        }

        // URL seeding functionality
        function seedUrl() {
            var url = document.getElementById("seed-url").value;
            if (!url) {
                document.getElementById("seed-result").innerHTML = '<div class="error">Please enter a URL</div>';
                return;
            }

            // Show loading message
            document.getElementById("seed-result").innerHTML = '<div>Seeding URL...</div>';

            // Send URL to server
            fetch('/seed', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/x-www-form-urlencoded',
                },
                body: 'url=' + encodeURIComponent(url)
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    document.getElementById("seed-result").innerHTML = '<div class="success">' + data.message + '</div>';
                    document.getElementById("seed-url").value = '';
                } else {
                    document.getElementById("seed-result").innerHTML = '<div class="error">' + data.message + '</div>';
                }
            })
            .catch(error => {
                document.getElementById("seed-result").innerHTML = '<div class="error">Error: ' + error + '</div>';
            });
        }

        // AWS metrics loading functionality
        function loadAwsMetrics() {
            // Check AWS service status
            fetch('/api/aws/status')
                .then(response => response.json())
                .then(data => {
                    if (!data.aws_available) {
                        const errorMsg = '<div class="error">AWS services are not available. Make sure your AWS credentials are configured correctly.</div>';
                        document.getElementById("sqs-metrics").innerHTML = errorMsg;
                        document.getElementById("dynamodb-tables").innerHTML = errorMsg;
                        document.getElementById("s3-metrics").innerHTML = errorMsg;
                        document.getElementById("ec2-instances").innerHTML = errorMsg;
                        document.getElementById("cloudwatch-logs").innerHTML = errorMsg;

                        // Also update management sections
                        document.getElementById("ec2-management-list").innerHTML = errorMsg;
                        document.getElementById("dynamodb-management-list").innerHTML = errorMsg;
                        document.getElementById("purge-url-queue-result").innerHTML = errorMsg;
                        document.getElementById("purge-status-queue-result").innerHTML = errorMsg;
                        document.getElementById("empty-bucket-result").innerHTML = errorMsg;
                        document.getElementById("reset-all-result").innerHTML = errorMsg;
                        return;
                    }

                    // Load SQS metrics
                    loadSqsMetrics();

                    // Load DynamoDB metrics
                    loadDynamoDbMetrics();

                    // Load S3 metrics
                    loadS3Metrics();

                    // Load EC2 metrics
                    loadEc2Metrics();

                    // Load CloudWatch logs
                    loadCloudWatchLogs();
                })
                .catch(error => {
                    console.error('Error checking AWS status:', error);
                    const errorMsg = '<div class="error">Error connecting to server. Please check if the server is running correctly.</div>';
                    document.getElementById("sqs-metrics").innerHTML = errorMsg;
                    document.getElementById("dynamodb-tables").innerHTML = errorMsg;
                    document.getElementById("s3-metrics").innerHTML = errorMsg;
                    document.getElementById("ec2-instances").innerHTML = errorMsg;
                    document.getElementById("cloudwatch-logs").innerHTML = errorMsg;
                });
        }

        // Load SQS metrics
        function loadSqsMetrics() {
            fetch('/api/aws/sqs')
                .then(response => response.json())
                .then(data => {
                    if (data.error) {
                        document.getElementById("sqs-metrics").innerHTML = '<div class="error">' + data.error + '</div>';
                        document.getElementById("sqs-messages").innerHTML = '<div class="error">' + data.error + '</div>';
                        return;
                    }

                    // Update URL queue metrics
                    document.getElementById("url-queue-messages").textContent = data.url_queue.messages_available;

                    // Update status queue metrics
                    document.getElementById("status-queue-messages").textContent = data.status_queue.messages_available;

                    // Update sample messages
                    var messagesHtml = '';
                    if (data.url_queue.sample_messages.length === 0) {
                        messagesHtml = '<div>No messages in queue</div>';
                    } else {
                        for (var i = 0; i < data.url_queue.sample_messages.length; i++) {
                            messagesHtml += '<div class="log-entry">' + data.url_queue.sample_messages[i].body + '</div>';
                        }
                    }
                    document.getElementById("sqs-messages").innerHTML = messagesHtml;
                })
                .catch(error => {
                    console.error('Error loading SQS metrics:', error);
                    document.getElementById("sqs-metrics").innerHTML = '<div class="error">Error loading SQS metrics</div>';
                });
        }

        // Load DynamoDB metrics
        function loadDynamoDbMetrics() {
            fetch('/api/aws/dynamodb')
                .then(response => response.json())
                .then(data => {
                    if (data.error) {
                        document.getElementById("dynamodb-tables").innerHTML = '<div class="error">' + data.error + '</div>';
                        return;
                    }

                    var tablesHtml = '';
                    var tableCount = 0;

                    for (var table in data) {
                        tableCount++;
                        tablesHtml += '<div class="flex-item"><div class="metrics-card">';
                        tablesHtml += '<div class="metrics-title">' + table + '</div>';
                        tablesHtml += '<div>Items: ' + data[table].item_count + '</div>';
                        tablesHtml += '<div>Size: ' + formatBytes(data[table].size_bytes) + '</div>';
                        tablesHtml += '<div>Status: ' + data[table].status + '</div>';
                        tablesHtml += '</div></div>';
                    }

                    if (tableCount === 0) {
                        tablesHtml = '<div>No DynamoDB tables found</div>';
                    }

                    document.getElementById("dynamodb-tables").innerHTML = tablesHtml;
                })
                .catch(error => {
                    console.error('Error loading DynamoDB metrics:', error);
                    document.getElementById("dynamodb-tables").innerHTML = '<div class="error">Error loading DynamoDB metrics</div>';
                });
        }

        // Load S3 metrics
        function loadS3Metrics() {
            fetch('/api/aws/s3')
                .then(response => response.json())
                .then(data => {
                    if (data.error) {
                        document.getElementById("s3-metrics").innerHTML = '<div class="error">' + data.error + '</div>';
                        document.getElementById("s3-objects").innerHTML = '<div class="error">' + data.error + '</div>';
                        return;
                    }

                    // Update object count
                    document.getElementById("s3-object-count").textContent = data.object_count;

                    // Update total size
                    document.getElementById("s3-total-size").textContent = formatBytes(data.total_size);

                    // Update recent objects
                    var objectsHtml = '';
                    if (data.recent_objects.length === 0) {
                        objectsHtml = '<div>No objects in bucket</div>';
                    } else {
                        for (var i = 0; i < data.recent_objects.length; i++) {
                            var obj = data.recent_objects[i];
                            objectsHtml += '<div class="log-entry">';
                            objectsHtml += '<div><strong>Key:</strong> ' + obj.key + '</div>';
                            objectsHtml += '<div><strong>Size:</strong> ' + formatBytes(obj.size) + '</div>';
                            objectsHtml += '<div><strong>Last Modified:</strong> ' + obj.last_modified + '</div>';
                            objectsHtml += '</div>';
                        }
                    }
                    document.getElementById("s3-objects").innerHTML = objectsHtml;
                })
                .catch(error => {
                    console.error('Error loading S3 metrics:', error);
                    document.getElementById("s3-metrics").innerHTML = '<div class="error">Error loading S3 metrics</div>';
                });
        }

        // Load EC2 metrics
        function loadEc2Metrics() {
            fetch('/api/aws/ec2')
                .then(response => response.json())
                .then(data => {
                    if (data.error) {
                        document.getElementById("ec2-instances").innerHTML = '<div class="error">' + data.error + '</div>';
                        return;
                    }

                    var instancesHtml = '';
                    if (data.length === 0) {
                        instancesHtml = '<div>No EC2 instances found</div>';
                    } else {
                        instancesHtml = '<table class="table"><thead><tr>';
                        instancesHtml += '<th>ID</th><th>Role</th><th>State</th><th>Type</th><th>Public IP</th>';
                        instancesHtml += '</tr></thead><tbody>';

                        for (var i = 0; i < data.length; i++) {
                            var instance = data[i];
                            instancesHtml += '<tr>';
                            instancesHtml += '<td>' + (instance.id || '') + '</td>';
                            instancesHtml += '<td>' + (instance.role || '') + '</td>';
                            instancesHtml += '<td>' + (instance.state || '') + '</td>';
                            instancesHtml += '<td>' + (instance.type || '') + '</td>';
                            instancesHtml += '<td>' + (instance.public_ip || '') + '</td>';
                            instancesHtml += '</tr>';
                        }

                        instancesHtml += '</tbody></table>';
                    }

                    document.getElementById("ec2-instances").innerHTML = instancesHtml;
                })
                .catch(error => {
                    console.error('Error loading EC2 metrics:', error);
                    document.getElementById("ec2-instances").innerHTML = '<div class="error">Error loading EC2 metrics</div>';
                });
        }

        // Load CloudWatch logs
        function loadCloudWatchLogs() {
            fetch('/api/aws/logs')
                .then(response => response.json())
                .then(data => {
                    if (data.error) {
                        document.getElementById("cloudwatch-logs").innerHTML = '<div class="error">' + data.error + '</div>';
                        return;
                    }

                    var logsHtml = '';
                    var logGroupCount = 0;

                    for (var groupName in data) {
                        logGroupCount++;
                        logsHtml += '<div class="log-group">';
                        logsHtml += '<h4>' + groupName + '</h4>';

                        for (var i = 0; i < data[groupName].length; i++) {
                            var stream = data[groupName][i];
                            logsHtml += '<div class="log-stream">';
                            logsHtml += '<h5>' + stream.stream + '</h5>';

                            for (var j = 0; j < stream.events.length; j++) {
                                var event = stream.events[j];
                                var date = new Date(event.timestamp);
                                logsHtml += '<div class="log-entry">';
                                logsHtml += '<span class="log-timestamp">' + date.toISOString() + '</span> ';
                                logsHtml += '<span class="log-message">' + event.message + '</span>';
                                logsHtml += '</div>';
                            }

                            logsHtml += '</div>';
                        }

                        logsHtml += '</div>';
                    }

                    if (logGroupCount === 0) {
                        logsHtml = '<div>No CloudWatch logs found</div>';
                    }

                    document.getElementById("cloudwatch-logs").innerHTML = logsHtml;
                })
                .catch(error => {
                    console.error('Error loading CloudWatch logs:', error);
                    document.getElementById("cloudwatch-logs").innerHTML = '<div class="error">Error loading CloudWatch logs</div>';
                });
        }

        // Format bytes to human-readable format
        function formatBytes(bytes, decimals = 2) {
            if (bytes === 0) return '0 Bytes';

            const k = 1024;
            const dm = decimals < 0 ? 0 : decimals;
            const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];

            const i = Math.floor(Math.log(bytes) / Math.log(k));

            return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
        }

        // Auto-refresh AWS metrics every 30 seconds
        setInterval(function() {
            loadAwsMetrics();
        }, 30000);

        // This event listener is now added in the DOMContentLoaded event

        // AWS Management Functions

        // Load EC2 instances for management
        function loadEc2Management() {
            fetch('/api/aws/ec2/list')
                .then(response => response.json())
                .then(data => {
                    if (data.error) {
                        document.getElementById("ec2-management-list").innerHTML = '<div class="error">' + data.error + '</div>';
                        return;
                    }

                    var instancesHtml = '';
                    if (data.length === 0) {
                        instancesHtml = '<div class="info-message">No EC2 instances found. You may need to create instances for your crawler system.</div>';
                    } else {
                        for (var i = 0; i < data.length; i++) {
                            var instance = data[i];
                            instancesHtml += '<div class="instance-card">';
                            instancesHtml += '<div><strong>ID:</strong> ' + instance.id + '</div>';
                            instancesHtml += '<div><strong>Role:</strong> ' + instance.role + '</div>';
                            instancesHtml += '<div><strong>State:</strong> ' + instance.state + '</div>';
                            instancesHtml += '<div><strong>Type:</strong> ' + instance.type + '</div>';
                            instancesHtml += '<div><strong>Public IP:</strong> ' + instance.public_ip + '</div>';

                            instancesHtml += '<div class="instance-actions">';
                            if (instance.state === 'running') {
                                instancesHtml += '<button class="action-button stop-instance-btn" data-instance-id="' + instance.id + '">Stop</button> ';
                                instancesHtml += '<button class="action-button restart-instance-btn" data-instance-id="' + instance.id + '">Restart</button>';
                            } else if (instance.state === 'stopped') {
                                instancesHtml += '<button class="action-button start-instance-btn" data-instance-id="' + instance.id + '">Start</button>';
                            } else {
                                instancesHtml += '<button disabled class="action-button">Instance is ' + instance.state + '</button>';
                            }
                            instancesHtml += '</div>';

                            instancesHtml += '<div id="' + instance.id + '-result"></div>';
                            instancesHtml += '</div>';
                        }
                    }

                    document.getElementById("ec2-management-list").innerHTML = instancesHtml;

                    // Add event listeners for EC2 instance management buttons
                    document.querySelectorAll('.start-instance-btn').forEach(function(button) {
                        button.addEventListener('click', function() {
                            var instanceId = this.getAttribute('data-instance-id');
                            startEc2Instance(instanceId);
                        });
                    });

                    document.querySelectorAll('.stop-instance-btn').forEach(function(button) {
                        button.addEventListener('click', function() {
                            var instanceId = this.getAttribute('data-instance-id');
                            stopEc2Instance(instanceId);
                        });
                    });

                    document.querySelectorAll('.restart-instance-btn').forEach(function(button) {
                        button.addEventListener('click', function() {
                            var instanceId = this.getAttribute('data-instance-id');
                            restartEc2Instance(instanceId);
                        });
                    });
                })
                .catch(error => {
                    console.error('Error loading EC2 instances for management:', error);
                    document.getElementById("ec2-management-list").innerHTML = '<div class="error">Error loading EC2 instances. Check your AWS credentials and network connection.</div>';
                });
        }

        // Load DynamoDB tables for management
        function loadDynamoDbManagement() {
            fetch('/api/aws/dynamodb')
                .then(response => response.json())
                .then(data => {
                    if (data.error) {
                        document.getElementById("dynamodb-management-list").innerHTML = '<div class="error">' + data.error + '</div>';
                        return;
                    }

                    var tablesHtml = '';
                    var tableCount = 0;

                    for (var table in data) {
                        tableCount++;
                        // Create a safe ID by removing special characters
                        var safeTableName = table.replace(/[^a-zA-Z0-9]/g, '');

                        tablesHtml += '<div class="metrics-card">';
                        tablesHtml += '<div class="metrics-title">' + table + '</div>';
                        tablesHtml += '<p>Clear all items from this table.</p>';
                        tablesHtml += '<div>Items: ' + data[table].item_count + '</div>';
                        tablesHtml += '<div>Size: ' + formatBytes(data[table].size_bytes) + '</div>';
                        tablesHtml += '<button class="action-button clear-table-btn" data-table="' + table + '">Clear Table</button>';
                        tablesHtml += '<div id="clear-' + safeTableName + '-result"></div>';
                        tablesHtml += '</div>';
                    }

                    if (tableCount === 0) {
                        tablesHtml = '<div class="info-message">No DynamoDB tables found. You may need to create tables for your crawler system.</div>';
                    }

                    document.getElementById("dynamodb-management-list").innerHTML = tablesHtml;

                    // Add event listeners for the Clear Table buttons
                    document.querySelectorAll('.clear-table-btn').forEach(function(button) {
                        button.addEventListener('click', function() {
                            var tableName = this.getAttribute('data-table');
                            clearDynamoDbTable(tableName);
                        });
                    });
                })
                .catch(error => {
                    console.error('Error loading DynamoDB tables for management:', error);
                    document.getElementById("dynamodb-management-list").innerHTML = '<div class="error">Error loading DynamoDB tables. Check your AWS credentials and network connection.</div>';
                });
        }

        // Start EC2 instance
        function startEc2Instance(instanceId) {
            if (!confirm('Are you sure you want to start instance ' + instanceId + '?')) {
                return;
            }

            document.getElementById(instanceId + '-result').innerHTML = '<div>Starting instance...</div>';

            fetch('/api/aws/ec2/start/' + instanceId, {
                method: 'POST'
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    document.getElementById(instanceId + '-result').innerHTML = '<div class="success">' + data.message + '</div>';
                    // Reload EC2 instances after a delay
                    setTimeout(loadEc2Management, 2000);
                } else {
                    document.getElementById(instanceId + '-result').innerHTML = '<div class="error">' + data.error + '</div>';
                }
            })
            .catch(error => {
                document.getElementById(instanceId + '-result').innerHTML = '<div class="error">Error: ' + error + '</div>';
            });
        }

        // Stop EC2 instance
        function stopEc2Instance(instanceId) {
            if (!confirm('Are you sure you want to stop instance ' + instanceId + '?')) {
                return;
            }

            document.getElementById(instanceId + '-result').innerHTML = '<div>Stopping instance...</div>';

            fetch('/api/aws/ec2/stop/' + instanceId, {
                method: 'POST'
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    document.getElementById(instanceId + '-result').innerHTML = '<div class="success">' + data.message + '</div>';
                    // Reload EC2 instances after a delay
                    setTimeout(loadEc2Management, 2000);
                } else {
                    document.getElementById(instanceId + '-result').innerHTML = '<div class="error">' + data.error + '</div>';
                }
            })
            .catch(error => {
                document.getElementById(instanceId + '-result').innerHTML = '<div class="error">Error: ' + error + '</div>';
            });
        }

        // Restart EC2 instance
        function restartEc2Instance(instanceId) {
            if (!confirm('Are you sure you want to restart instance ' + instanceId + '?')) {
                return;
            }

            document.getElementById(instanceId + '-result').innerHTML = '<div>Restarting instance...</div>';

            fetch('/api/aws/ec2/restart/' + instanceId, {
                method: 'POST'
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    document.getElementById(instanceId + '-result').innerHTML = '<div class="success">' + data.message + '</div>';
                    // Reload EC2 instances after a delay
                    setTimeout(loadEc2Management, 2000);
                } else {
                    document.getElementById(instanceId + '-result').innerHTML = '<div class="error">' + data.error + '</div>';
                }
            })
            .catch(error => {
                document.getElementById(instanceId + '-result').innerHTML = '<div class="error">Error: ' + error + '</div>';
            });
        }

        // Purge SQS queue
        function purgeSqsQueue(queueName) {
            if (!confirm('Are you sure you want to purge all messages from the ' + queueName + ' queue?')) {
                return;
            }

            // Create a safe ID by removing special characters
            var safeQueueName = queueName.replace(/[^a-zA-Z0-9]/g, '');
            var resultElementId = 'purge-' + safeQueueName + '-result';
            var resultElement = document.getElementById(resultElementId);

            if (!resultElement) {
                console.error('Element with ID ' + resultElementId + ' not found');
                return;
            }

            resultElement.innerHTML = '<div>Purging queue...</div>';

            fetch('/api/aws/sqs/purge/' + queueName, {
                method: 'POST'
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    var element = document.getElementById(resultElementId);
                    if (element) {
                        element.innerHTML = '<div class="success">' + data.message + '</div>';
                    }
                    // Reload SQS metrics after a delay
                    setTimeout(loadSqsMetrics, 2000);
                } else {
                    var element = document.getElementById(resultElementId);
                    if (element) {
                        element.innerHTML = '<div class="error">' + data.error + '</div>';
                    }
                }
            })
            .catch(error => {
                var element = document.getElementById(resultElementId);
                if (element) {
                    element.innerHTML = '<div class="error">Error: ' + error + '</div>';
                }
            });
        }

        // Clear DynamoDB table
        function clearDynamoDbTable(tableName) {
            if (!confirm('Are you sure you want to clear all items from the ' + tableName + ' table?')) {
                return;
            }

            // Create a safe ID by removing special characters
            var safeTableName = tableName.replace(/[^a-zA-Z0-9]/g, '');
            var resultElementId = 'clear-' + safeTableName + '-result';
            var resultElement = document.getElementById(resultElementId);

            if (!resultElement) {
                console.error('Element with ID ' + resultElementId + ' not found');
                return;
            }

            resultElement.innerHTML = '<div>Clearing table...</div>';

            fetch('/api/aws/dynamodb/clear/' + tableName, {
                method: 'POST'
            })
            .then(response => response.json())
            .then(data => {
                var element = document.getElementById(resultElementId);
                if (element) {
                    if (data.success) {
                        element.innerHTML = '<div class="success">' + data.message + '</div>';
                        // Reload DynamoDB metrics after a delay
                        setTimeout(function() {
                            loadDynamoDbMetrics();
                            loadDynamoDbManagement();
                        }, 2000);
                    } else {
                        element.innerHTML = '<div class="error">' + data.error + '</div>';
                    }
                }
            })
            .catch(error => {
                var element = document.getElementById(resultElementId);
                if (element) {
                    element.innerHTML = '<div class="error">Error: ' + error + '</div>';
                }
            });
        }

        // Empty S3 bucket
        function emptyS3Bucket(bucketName) {
            if (!confirm('Are you sure you want to empty the ' + bucketName + ' bucket? This will delete all objects in the bucket.')) {
                return;
            }

            // Create a safe ID by removing special characters
            var safeBucketName = bucketName.replace(/[^a-zA-Z0-9]/g, '');
            var resultElementId = 'empty-' + safeBucketName + '-result';
            var resultElement = document.getElementById(resultElementId);

            if (!resultElement) {
                console.error('Element with ID ' + resultElementId + ' not found');
                // Fall back to the default element
                resultElement = document.getElementById('empty-bucket-result');
                if (!resultElement) {
                    console.error('Fallback element with ID empty-bucket-result not found');
                    return;
                }
            }

            resultElement.innerHTML = '<div>Emptying bucket...</div>';

            fetch('/api/aws/s3/empty/' + bucketName, {
                method: 'POST'
            })
            .then(response => response.json())
            .then(data => {
                var element = document.getElementById(resultElementId) || document.getElementById('empty-bucket-result');
                if (element) {
                    if (data.success) {
                        element.innerHTML = '<div class="success">' + data.message + '</div>';
                        // Reload S3 metrics after a delay
                        setTimeout(loadS3Metrics, 2000);
                    } else {
                        element.innerHTML = '<div class="error">' + data.error + '</div>';
                    }
                }
            })
            .catch(error => {
                var element = document.getElementById(resultElementId) || document.getElementById('empty-bucket-result');
                if (element) {
                    element.innerHTML = '<div class="error">Error: ' + error + '</div>';
                }
            });
        }

        // Reset all resources
        function resetAllResources() {
            if (!confirm('WARNING: This will reset all AWS resources related to the crawler system. This action cannot be undone. Are you sure you want to proceed?')) {
                return;
            }

            var resultElement = document.getElementById('reset-all-result');
            if (!resultElement) {
                console.error('Element with ID reset-all-result not found');
                return;
            }

            resultElement.innerHTML = '<div>Resetting all resources...</div>';

            fetch('/api/aws/reset', {
                method: 'POST'
            })
            .then(response => response.json())
            .then(data => {
                var element = document.getElementById('reset-all-result');
                if (element) {
                    if (data.success) {
                        element.innerHTML = '<div class="success">' + data.message + '</div>';
                        // Reload all metrics after a delay
                        setTimeout(function() {
                            loadSqsMetrics();
                            loadDynamoDbMetrics();
                            loadS3Metrics();
                            loadDynamoDbManagement();
                        }, 2000);
                    } else {
                        element.innerHTML = '<div class="error">' + data.error + '</div>';
                    }
                }
            })
            .catch(error => {
                var element = document.getElementById('reset-all-result');
                if (element) {
                    element.innerHTML = '<div class="error">Error: ' + error + '</div>';
                }
            });
        }

        // This has been replaced by the new DOMContentLoaded event handler above
    </script>
</body>
</html>
""")

def normalize_url(url):
    """Normalize URL to ensure consistent format."""
    if not url:
        return None

    # Add http:// if no scheme is provided
    if not url.startswith(('http://', 'https://')):
        url = 'http://' + url

    # Parse the URL
    parsed = urlparse(url)

    # Ensure the URL has a valid domain
    if not parsed.netloc:
        return None

    # Return the normalized URL
    return url

def seed_url(url):
    """Seed a URL to the crawler queue."""
    if not aws_services_available:
        return False, "AWS services are not available"

    # Normalize the URL
    normalized_url = normalize_url(url)
    if not normalized_url:
        return False, "Invalid URL format"

    try:
        # Add URL to tracking table
        db_manager.add_url_to_tracking(normalized_url)

        # Send URL to SQS queue
        success = task_queue.send_message(normalized_url)

        if success:
            logging.info(f"Successfully seeded URL: {normalized_url}")
            return True, f"URL successfully added to crawler queue: {normalized_url}"
        else:
            logging.error(f"Failed to send URL to SQS: {normalized_url}")
            return False, "Failed to send URL to SQS queue"

    except Exception as e:
        logging.error(f"Error seeding URL {normalized_url}: {e}")
        logging.debug(traceback.format_exc())
        return False, f"Error: {str(e)}"

def get_sqs_metrics():
    """Get metrics for SQS queues."""
    if not aws_services_available:
        return {"error": "AWS services are not available"}

    try:
        # Get URL queue metrics
        url_queue_response = sqs_client.get_queue_url(QueueName='crawler-url-queue')
        url_queue_url = url_queue_response['QueueUrl']

        url_queue_attrs = sqs_client.get_queue_attributes(
            QueueUrl=url_queue_url,
            AttributeNames=['All']
        )['Attributes']

        # Get status queue metrics
        status_queue_response = sqs_client.get_queue_url(QueueName='crawler-status-queue')
        status_queue_url = status_queue_response['QueueUrl']

        status_queue_attrs = sqs_client.get_queue_attributes(
            QueueUrl=status_queue_url,
            AttributeNames=['All']
        )['Attributes']

        # Sample messages from URL queue (without removing them)
        sample_messages = []
        try:
            messages = sqs_client.receive_message(
                QueueUrl=url_queue_url,
                MaxNumberOfMessages=5,
                VisibilityTimeout=5,  # Short timeout to return to queue quickly
                WaitTimeSeconds=1
            ).get('Messages', [])

            for msg in messages:
                sample_messages.append({
                    'body': msg.get('Body', ''),
                    'id': msg.get('MessageId', '')
                })
        except Exception as e:
            logging.error(f"Error sampling SQS messages: {e}")

        return {
            'url_queue': {
                'url': url_queue_url,
                'messages_available': url_queue_attrs.get('ApproximateNumberOfMessages', '0'),
                'messages_in_flight': url_queue_attrs.get('ApproximateNumberOfMessagesNotVisible', '0'),
                'created': url_queue_attrs.get('CreatedTimestamp', ''),
                'sample_messages': sample_messages
            },
            'status_queue': {
                'url': status_queue_url,
                'messages_available': status_queue_attrs.get('ApproximateNumberOfMessages', '0'),
                'messages_in_flight': status_queue_attrs.get('ApproximateNumberOfMessagesNotVisible', '0'),
                'created': status_queue_attrs.get('CreatedTimestamp', '')
            }
        }
    except Exception as e:
        logging.error(f"Error getting SQS metrics: {e}")
        logging.debug(traceback.format_exc())
        return {"error": str(e)}

def get_dynamodb_metrics():
    """Get metrics for DynamoDB tables."""
    if not aws_services_available:
        return {"error": "AWS services are not available"}

    try:
        tables = dynamodb_client.list_tables()['TableNames']
        table_metrics = {}

        for table in tables:
            if table.startswith('crawler-'):
                table_desc = dynamodb_client.describe_table(TableName=table)['Table']

                # Get item count (approximate)
                item_count = table_desc.get('ItemCount', 0)

                # Get table size
                table_size = table_desc.get('TableSizeBytes', 0)

                # Sample items from the table
                sample_items = []
                try:
                    scan_result = dynamodb_client.scan(
                        TableName=table,
                        Limit=5
                    )
                    sample_items = scan_result.get('Items', [])
                except Exception as e:
                    logging.error(f"Error scanning DynamoDB table {table}: {e}")

                # Format creation date if it's a datetime object
                creation_date = table_desc.get('CreationDateTime', '')
                if hasattr(creation_date, 'isoformat'):
                    creation_date = creation_date.isoformat()

                table_metrics[table] = {
                    'item_count': item_count,
                    'size_bytes': table_size,
                    'status': table_desc.get('TableStatus', ''),
                    'creation_date': creation_date,
                    'sample_items': sample_items
                }

        return table_metrics
    except Exception as e:
        logging.error(f"Error getting DynamoDB metrics: {e}")
        logging.debug(traceback.format_exc())
        return {"error": str(e)}

def get_s3_metrics():
    """Get metrics for S3 bucket."""
    if not aws_services_available:
        return {"error": "AWS services are not available"}

    try:
        bucket_name = 'web-crawler-data-storage'

        # Check if bucket exists
        try:
            s3_client.head_bucket(Bucket=bucket_name)
        except Exception:
            return {"error": f"Bucket {bucket_name} does not exist or is not accessible"}

        # Get bucket metrics
        objects = s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=100)

        total_size = 0
        object_count = 0
        recent_objects = []

        if 'Contents' in objects:
            object_count = len(objects['Contents'])

            # Calculate total size and get recent objects
            for obj in objects['Contents']:
                total_size += obj.get('Size', 0)

                # Format last_modified date
                last_modified = obj.get('LastModified', '')
                if hasattr(last_modified, 'isoformat'):
                    last_modified = last_modified.isoformat()
                else:
                    last_modified = str(last_modified)

                # Add recent objects (last 10)
                if len(recent_objects) < 10:
                    recent_objects.append({
                        'key': obj.get('Key', ''),
                        'size': obj.get('Size', 0),
                        'last_modified': last_modified
                    })

        return {
            'bucket_name': bucket_name,
            'object_count': object_count,
            'total_size': total_size,
            'recent_objects': recent_objects
        }
    except Exception as e:
        logging.error(f"Error getting S3 metrics: {e}")
        logging.debug(traceback.format_exc())
        return {"error": str(e)}

def get_ec2_metrics():
    """Get metrics for EC2 instances."""
    if not aws_services_available:
        return {"error": "AWS services are not available"}

    try:
        # Get all instances with crawler tag
        instances = ec2_client.describe_instances(
            Filters=[
                {
                    'Name': 'tag:Project',
                    'Values': ['WebCrawler']
                }
            ]
        )

        instance_metrics = []

        for reservation in instances.get('Reservations', []):
            for instance in reservation.get('Instances', []):
                # Get instance details
                instance_id = instance.get('InstanceId', '')
                state = instance.get('State', {}).get('Name', '')
                instance_type = instance.get('InstanceType', '')
                launch_time = instance.get('LaunchTime', '').isoformat() if hasattr(instance.get('LaunchTime', ''), 'isoformat') else str(instance.get('LaunchTime', ''))

                # Get instance role from tags
                role = 'unknown'
                for tag in instance.get('Tags', []):
                    if tag.get('Key') == 'Role':
                        role = tag.get('Value', 'unknown')

                instance_metrics.append({
                    'id': instance_id,
                    'state': state,
                    'type': instance_type,
                    'launch_time': launch_time,
                    'role': role,
                    'public_ip': instance.get('PublicIpAddress', ''),
                    'private_ip': instance.get('PrivateIpAddress', '')
                })

        return instance_metrics
    except Exception as e:
        logging.error(f"Error getting EC2 metrics: {e}")
        logging.debug(traceback.format_exc())
        return {"error": str(e)}

def get_cloudwatch_logs():
    """Get recent CloudWatch logs."""
    if not aws_services_available:
        return {"error": "AWS services are not available"}

    try:
        # Get log groups
        log_groups = logs_client.describe_log_groups(
            logGroupNamePrefix='/crawler'
        ).get('logGroups', [])

        logs_data = {}

        for group in log_groups:
            group_name = group.get('logGroupName', '')

            # Get log streams for this group
            streams = logs_client.describe_log_streams(
                logGroupName=group_name,
                orderBy='LastEventTime',
                descending=True,
                limit=5
            ).get('logStreams', [])

            group_logs = []

            for stream in streams:
                stream_name = stream.get('logStreamName', '')

                # Get log events for this stream
                events = logs_client.get_log_events(
                    logGroupName=group_name,
                    logStreamName=stream_name,
                    limit=20,
                    startFromHead=False
                ).get('events', [])

                stream_events = []
                for event in events:
                    stream_events.append({
                        'timestamp': event.get('timestamp', 0),
                        'message': event.get('message', '')
                    })

                if stream_events:
                    group_logs.append({
                        'stream': stream_name,
                        'events': stream_events
                    })

            if group_logs:
                logs_data[group_name] = group_logs

        return logs_data
    except Exception as e:
        logging.error(f"Error getting CloudWatch logs: {e}")
        logging.debug(traceback.format_exc())
        return {"error": str(e)}

def list_crawler_ec2_instances():
    """List all EC2 instances related to the crawler system."""
    if not aws_services_available:
        return {"error": "AWS services are not available"}

    try:
        # Get all instances with crawler tag
        instances = ec2_client.describe_instances(
            Filters=[
                {
                    'Name': 'tag:Project',
                    'Values': ['WebCrawler']
                }
            ]
        )

        instance_list = []

        for reservation in instances.get('Reservations', []):
            for instance in reservation.get('Instances', []):
                # Get instance details
                instance_id = instance.get('InstanceId', '')
                state = instance.get('State', {}).get('Name', '')
                instance_type = instance.get('InstanceType', '')
                launch_time = instance.get('LaunchTime', '').isoformat() if hasattr(instance.get('LaunchTime', ''), 'isoformat') else str(instance.get('LaunchTime', ''))

                # Get instance role from tags
                role = 'unknown'
                for tag in instance.get('Tags', []):
                    if tag.get('Key') == 'Role':
                        role = tag.get('Value', 'unknown')

                instance_list.append({
                    'id': instance_id,
                    'state': state,
                    'type': instance_type,
                    'launch_time': launch_time,
                    'role': role,
                    'public_ip': instance.get('PublicIpAddress', ''),
                    'private_ip': instance.get('PrivateIpAddress', '')
                })

        return instance_list
    except Exception as e:
        logging.error(f"Error listing EC2 instances: {e}")
        logging.debug(traceback.format_exc())
        return {"error": str(e)}

def start_ec2_instance(instance_id):
    """Start an EC2 instance."""
    if not aws_services_available:
        return {"success": False, "error": "AWS services are not available"}

    try:
        # Check if instance exists and belongs to crawler project
        instances = list_crawler_ec2_instances()
        if isinstance(instances, dict) and "error" in instances:
            return {"success": False, "error": instances["error"]}

        instance_ids = [instance['id'] for instance in instances]
        if instance_id not in instance_ids:
            return {"success": False, "error": f"Instance {instance_id} not found or not part of crawler project"}

        # Start the instance
        response = ec2_client.start_instances(InstanceIds=[instance_id])

        # Check response
        if response.get('StartingInstances'):
            return {"success": True, "message": f"Instance {instance_id} starting"}
        else:
            return {"success": False, "error": "Failed to start instance"}
    except Exception as e:
        logging.error(f"Error starting EC2 instance {instance_id}: {e}")
        logging.debug(traceback.format_exc())
        return {"success": False, "error": str(e)}

def stop_ec2_instance(instance_id):
    """Stop an EC2 instance."""
    if not aws_services_available:
        return {"success": False, "error": "AWS services are not available"}

    try:
        # Check if instance exists and belongs to crawler project
        instances = list_crawler_ec2_instances()
        if isinstance(instances, dict) and "error" in instances:
            return {"success": False, "error": instances["error"]}

        instance_ids = [instance['id'] for instance in instances]
        if instance_id not in instance_ids:
            return {"success": False, "error": f"Instance {instance_id} not found or not part of crawler project"}

        # Stop the instance
        response = ec2_client.stop_instances(InstanceIds=[instance_id])

        # Check response
        if response.get('StoppingInstances'):
            return {"success": True, "message": f"Instance {instance_id} stopping"}
        else:
            return {"success": False, "error": "Failed to stop instance"}
    except Exception as e:
        logging.error(f"Error stopping EC2 instance {instance_id}: {e}")
        logging.debug(traceback.format_exc())
        return {"success": False, "error": str(e)}

def restart_ec2_instance(instance_id):
    """Restart an EC2 instance."""
    if not aws_services_available:
        return {"success": False, "error": "AWS services are not available"}

    try:
        # Check if instance exists and belongs to crawler project
        instances = list_crawler_ec2_instances()
        if isinstance(instances, dict) and "error" in instances:
            return {"success": False, "error": instances["error"]}

        instance_ids = [instance['id'] for instance in instances]
        if instance_id not in instance_ids:
            return {"success": False, "error": f"Instance {instance_id} not found or not part of crawler project"}

        # Restart the instance
        ec2_client.reboot_instances(InstanceIds=[instance_id])

        # No specific response for reboot, so check if there was no exception
        return {"success": True, "message": f"Instance {instance_id} restarting"}
    except Exception as e:
        logging.error(f"Error restarting EC2 instance {instance_id}: {e}")
        logging.debug(traceback.format_exc())
        return {"success": False, "error": str(e)}

def purge_sqs_queue(queue_name):
    """Purge all messages from an SQS queue."""
    if not aws_services_available:
        return {"success": False, "error": "AWS services are not available"}

    try:
        # Get queue URL
        queue_url_response = sqs_client.get_queue_url(QueueName=queue_name)
        queue_url = queue_url_response['QueueUrl']

        # Purge the queue
        sqs_client.purge_queue(QueueUrl=queue_url)

        return {"success": True, "message": f"Queue {queue_name} purged successfully"}
    except Exception as e:
        logging.error(f"Error purging SQS queue {queue_name}: {e}")
        logging.debug(traceback.format_exc())
        return {"success": False, "error": str(e)}

def clear_dynamodb_table(table_name):
    """Clear all items from a DynamoDB table."""
    if not aws_services_available:
        return {"success": False, "error": "AWS services are not available"}

    try:
        # Get table key schema
        table_description = dynamodb_client.describe_table(TableName=table_name)
        key_schema = table_description['Table']['KeySchema']

        # Get primary key name
        primary_key = next((item['AttributeName'] for item in key_schema if item['KeyType'] == 'HASH'), None)

        if not primary_key:
            return {"success": False, "error": f"Could not determine primary key for table {table_name}"}

        # Scan table for all items
        scan_response = dynamodb_client.scan(
            TableName=table_name,
            AttributesToGet=[primary_key]
        )

        items = scan_response.get('Items', [])

        # Delete each item
        deleted_count = 0
        for item in items:
            dynamodb_client.delete_item(
                TableName=table_name,
                Key={primary_key: item[primary_key]}
            )
            deleted_count += 1

        return {"success": True, "message": f"Deleted {deleted_count} items from table {table_name}"}
    except Exception as e:
        logging.error(f"Error clearing DynamoDB table {table_name}: {e}")
        logging.debug(traceback.format_exc())
        return {"success": False, "error": str(e)}

def empty_s3_bucket(bucket_name):
    """Empty all objects from an S3 bucket."""
    if not aws_services_available:
        return {"success": False, "error": "AWS services are not available"}

    try:
        # List all objects in the bucket
        objects = s3_client.list_objects_v2(Bucket=bucket_name)

        if 'Contents' not in objects:
            return {"success": True, "message": f"Bucket {bucket_name} is already empty"}

        # Delete all objects
        delete_list = {'Objects': [{'Key': obj['Key']} for obj in objects['Contents']]}
        s3_client.delete_objects(Bucket=bucket_name, Delete=delete_list)

        # Check if there are more objects (pagination)
        while objects['IsTruncated']:
            objects = s3_client.list_objects_v2(
                Bucket=bucket_name,
                ContinuationToken=objects['NextContinuationToken']
            )
            if 'Contents' in objects:
                delete_list = {'Objects': [{'Key': obj['Key']} for obj in objects['Contents']]}
                s3_client.delete_objects(Bucket=bucket_name, Delete=delete_list)

        return {"success": True, "message": f"Bucket {bucket_name} emptied successfully"}
    except Exception as e:
        logging.error(f"Error emptying S3 bucket {bucket_name}: {e}")
        logging.debug(traceback.format_exc())
        return {"success": False, "error": str(e)}

def reset_all_resources():
    """Reset all AWS resources related to the crawler system."""
    if not aws_services_available:
        return {"success": False, "error": "AWS services are not available"}

    results = {
        "sqs": {},
        "dynamodb": {},
        "s3": {}
    }

    # Reset SQS queues
    for queue_name in ['crawler-url-queue', 'crawler-status-queue']:
        results["sqs"][queue_name] = purge_sqs_queue(queue_name)

    # Reset DynamoDB tables
    tables = dynamodb_client.list_tables()['TableNames']
    for table in tables:
        if table.startswith('crawler-'):
            results["dynamodb"][table] = clear_dynamodb_table(table)

    # Reset S3 bucket
    bucket_name = 'web-crawler-data-storage'
    results["s3"][bucket_name] = empty_s3_bucket(bucket_name)

    return {
        "success": True,
        "message": "Reset operations completed",
        "details": results
    }

def search_index(query, fields=None, limit=10):
    """Search the index with the given query."""
    try:
        # Initialize the indexer
        indexer = WhooshIndexer()

        # Perform search
        results = indexer.search(query, fields=fields, limit=limit)

        return results, None
    except Exception as e:
        logging.error(f"Error searching index: {e}")
        logging.debug(traceback.format_exc())

        # Try to recover and search again with minimal fields
        try:
            logging.info("Attempting fallback search...")
            indexer = WhooshIndexer()  # Reinitialize

            # Try with just the title field
            results = indexer.search(query, fields=["title"], limit=limit)
            return results, None
        except Exception as e2:
            logging.error(f"Fallback search also failed: {e2}")
            return [], str(e2)

@app.route('/')
def search_page():
    """Render the search page and handle search requests."""
    query = request.args.get('q', '')

    if not query:
        # Just show the search form
        return render_template('search.html', query='', results=None, error=None)

    try:
        # Perform search
        results, error = search_index(query, limit=20)

        # Process results for display
        processed_results = []
        for result in results:
            # Handle missing fields gracefully
            processed_result = {
                'title': result.get('title', 'No title'),
                'url': result.get('url', '#'),
                'score': result.get('score', 0.0)
            }

            # Add optional fields if available
            if 'summary' in result and result['summary']:
                summary = result['summary']
                if len(summary) > 300:
                    summary = summary[:297] + "..."
                processed_result['summary'] = summary

            if 'keywords' in result and result['keywords']:
                keywords = result['keywords']
                if len(keywords) > 150:
                    keywords = keywords[:147] + "..."
                processed_result['keywords'] = keywords

            processed_results.append(processed_result)

        return render_template('search.html', query=query, results=processed_results, error=error)

    except Exception as e:
        logging.error(f"Error processing search request: {e}")
        logging.debug(traceback.format_exc())
        return render_template('search.html', query=query, results=None, error=str(e))

@app.route('/seed', methods=['POST'])
def seed_url_route():
    """Handle URL seeding requests."""
    url = request.form.get('url', '')

    if not url:
        return jsonify({'success': False, 'message': 'No URL provided'})

    success, message = seed_url(url)
    return jsonify({'success': success, 'message': message})

@app.route('/api/aws/sqs')
def api_sqs_metrics():
    """API endpoint for SQS metrics."""
    return jsonify(get_sqs_metrics())

@app.route('/api/aws/dynamodb')
def api_dynamodb_metrics():
    """API endpoint for DynamoDB metrics."""
    return jsonify(get_dynamodb_metrics())

@app.route('/api/aws/s3')
def api_s3_metrics():
    """API endpoint for S3 metrics."""
    return jsonify(get_s3_metrics())

@app.route('/api/aws/ec2')
def api_ec2_metrics():
    """API endpoint for EC2 metrics."""
    return jsonify(get_ec2_metrics())

@app.route('/api/aws/logs')
def api_cloudwatch_logs():
    """API endpoint for CloudWatch logs."""
    return jsonify(get_cloudwatch_logs())

@app.route('/api/aws/status')
def api_aws_status():
    """API endpoint for overall AWS service status."""
    return jsonify({
        'aws_available': aws_services_available,
        'sqs': {'available': aws_services_available},
        'dynamodb': {'available': aws_services_available},
        's3': {'available': aws_services_available},
        'ec2': {'available': aws_services_available},
        'cloudwatch': {'available': aws_services_available}
    })

@app.route('/api/aws/ec2/list')
def api_ec2_list():
    """API endpoint to list all EC2 instances."""
    return jsonify(list_crawler_ec2_instances())

@app.route('/api/aws/ec2/start/<instance_id>', methods=['POST'])
def api_ec2_start(instance_id):
    """API endpoint to start an EC2 instance."""
    return jsonify(start_ec2_instance(instance_id))

@app.route('/api/aws/ec2/stop/<instance_id>', methods=['POST'])
def api_ec2_stop(instance_id):
    """API endpoint to stop an EC2 instance."""
    return jsonify(stop_ec2_instance(instance_id))

@app.route('/api/aws/ec2/restart/<instance_id>', methods=['POST'])
def api_ec2_restart(instance_id):
    """API endpoint to restart an EC2 instance."""
    return jsonify(restart_ec2_instance(instance_id))

@app.route('/api/aws/sqs/purge/<queue_name>', methods=['POST'])
def api_sqs_purge(queue_name):
    """API endpoint to purge an SQS queue."""
    return jsonify(purge_sqs_queue(queue_name))

@app.route('/api/aws/dynamodb/clear/<table_name>', methods=['POST'])
def api_dynamodb_clear(table_name):
    """API endpoint to clear a DynamoDB table."""
    return jsonify(clear_dynamodb_table(table_name))

@app.route('/api/aws/s3/empty/<bucket_name>', methods=['POST'])
def api_s3_empty(bucket_name):
    """API endpoint to empty an S3 bucket."""
    return jsonify(empty_s3_bucket(bucket_name))

@app.route('/api/aws/reset', methods=['POST'])
def api_reset_all():
    """API endpoint to reset all AWS resources."""
    return jsonify(reset_all_resources())

@app.route('/api/search')
def api_search():
    """API endpoint for search."""
    query = request.args.get('q', '')
    limit = int(request.args.get('limit', 10))

    if not query:
        return jsonify({'error': 'No query provided'})

    try:
        # Perform search
        results, error = search_index(query, limit=limit)

        if error:
            return jsonify({'error': error})

        return jsonify({'results': results})

    except Exception as e:
        logging.error(f"Error processing API search request: {e}")
        return jsonify({'error': str(e)})

@app.errorhandler(400)
def bad_request_error(e):
    """Handle bad request errors."""
    logging.warning(f"Bad request: {e}")
    return render_template('search.html',
                          query='',
                          results=None,
                          error="Invalid request. Please try again with a valid request."), 400

@app.errorhandler(404)
def not_found_error(e):
    """Handle not found errors."""
    logging.warning(f"Page not found: {e}")
    return render_template('search.html',
                          query='',
                          results=None,
                          error="The requested page was not found."), 404

@app.errorhandler(500)
def internal_error(e):
    """Handle internal server errors."""
    logging.error(f"Internal server error: {e}")
    return render_template('search.html',
                          query='',
                          results=None,
                          error="An internal server error occurred. Please try again later."), 500

def main():
    """Main function to start the web server."""
    parser = argparse.ArgumentParser(description='Web interface for the crawler search')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--port', type=int, default=5000, help='Port to bind to')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')

    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    print(f"Starting web search interface on http://{args.host}:{args.port}")
    print("Press Ctrl+C to stop the server")

    # Set up Flask to handle malformed requests better
    app.config['TRAP_BAD_REQUEST_ERRORS'] = True
    app.config['TRAP_HTTP_EXCEPTIONS'] = True

    # Run the app
    app.run(host=args.host, port=args.port, debug=args.debug)

if __name__ == "__main__":
    main()
