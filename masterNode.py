#!/usr/bin/env python3
import time
import logging
import json
import os
from datetime import datetime, timedelta
from cloud_queue import CloudQueue
from cloud_storage import CloudStorage

# Define message types for AWS communication
MSG_TERMINATE = 'terminate'
MSG_URLS_DISCOVERED = 'urls_discovered'
MSG_INDEX_CONTENT = 'index_content'
MSG_HEARTBEAT = 'heartbeat'
MSG_ERROR = 'error'

def master_process(sqs_queue=None, status_queue=None, bucket=None):
    """
    Phase 3 Master Node:
      - Distributes crawling tasks (URLs) to the crawler nodes via cloud queue.
      - Receives extracted URLs and status messages from crawler nodes.
      - Implements fault tolerance through heartbeat monitoring and task re-queueing.
      - Tracks crawler node health and re-assigns tasks from failed nodes.
      - Records detailed system metrics for monitoring dashboard.
      - When work is complete, sends shutdown signals to crawler and indexer nodes.
    """
    # Initialize default queue and bucket names if not provided
    sqs_queue = sqs_queue or 'crawler-url-queue'
    status_queue = status_queue or 'crawler-status-queue'
    bucket = bucket or 'web-crawler-data-storage'

    # Enhanced logging for Phase 3
    log_file = os.path.join("logs", "master.log")
    # Ensure logs directory exists
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - Master - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    logging.info(f"Master node started")

    # Initialize the cloud queue and storage
    task_queue = CloudQueue(queue_name=sqs_queue, status_queue_name=status_queue)
    cloud_storage = CloudStorage(bucket_name=bucket)
    logging.info(f"Task queue initialized in {'cloud' if task_queue.is_cloud_mode() else 'local'} mode")
    logging.info(f"Cloud storage initialized with bucket: {bucket}")

    # Real seed URLs for testing
    seed_urls = [
        "https://www.python.org",
        "https://www.github.com",
        "https://www.wikipedia.org",
        "https://www.reddit.com",
        "https://www.stackoverflow.com"
    ]
    
    # Add seed URLs to the queue - just send them as plain strings
    # This is the simplest method that will work with the crawler
    for url in seed_urls:
        result = task_queue.send_message(url)
        logging.info(f"Added seed URL to queue: {url}, result: {result}")
    
    logging.info(f"Added {len(seed_urls)} seed URLs to the task queue")

    # Keep track of active crawler nodes - will be updated based on heartbeats
    active_crawler_nodes = {}  # Format: {node_id: last_heartbeat_time}
    max_urls_to_process = 50  # Increase from 20 to 1000 URLs to process
    min_runtime_seconds = 600   # Increase minimum runtime to 10 minutes

    processed_urls = 0

    # Task timeout and heartbeat tracking - Phase 3 fault tolerance
    heartbeat_timeout_seconds = 30  # Increase from 10 to 30 seconds for better fault tolerance
    
    # Monitoring metrics - for tracking system performance
    system_metrics = {
        "start_time": datetime.now().isoformat(),
        "urls_crawled": 0,
        "urls_indexed": 0,
        "urls_failed": 0,
        "crawler_status": {},
        "error_count": 0,
        "crawler_performance": {}
    }

    # Function to update monitoring data
    def update_monitoring_data():
        # Ensure data/monitoring directory exists
        monitoring_data_path = os.path.join("data", "monitoring", "monitoring_data.json")
        os.makedirs(os.path.dirname(monitoring_data_path), exist_ok=True)
        with open(monitoring_data_path, "w") as f:
            json.dump(system_metrics, f, indent=4)

    # Initialize monitoring data file
    update_monitoring_data()

    # Function to validate URL before sending to queue
    def is_valid_url(url):
        if not url or not isinstance(url, str):
            return False
        # Basic validation - ensure URL has a scheme
        return url.startswith('http://') or url.startswith('https://')

    # Initialize variables for URL discovery
    visited_urls = set(seed_urls)  # Track URLs but don't prevent initial crawling
    
    # Run for at least min_runtime_seconds regardless of queue size, to allow crawler nodes to connect
    start_time = datetime.now()
    min_run_time = timedelta(seconds=min_runtime_seconds)

    # Main processing loop
    logging.info("Starting main processing loop")
    queue_size = task_queue.get_queue_size()
    while ((queue_size > 0 or len(active_crawler_nodes) > 0) and 
           processed_urls < max_urls_to_process) or (datetime.now() - start_time < min_run_time):
        current_time = datetime.now()
        
        # Process status messages to get heartbeats and discover new nodes
        # IMPORTANT: Only receive from status queue, not task queue!
        status_messages = []
        try:
            # Only get messages from the status queue
            if task_queue.use_cloud:
                response = task_queue.sqs.receive_message(
                    QueueUrl=task_queue.status_queue_url,  # Use status queue URL
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=1,
                    VisibilityTimeout=30
                )
                status_messages = response.get('Messages', [])
            else:
                # Use local status queue for testing
                pass
        except Exception as e:
            logging.error(f"Error receiving status messages: {e}")
            
        for message in status_messages:
            try:
                message_body_raw = message.get('Body', '{}')
                
                # Skip empty messages
                if not message_body_raw or message_body_raw.strip() == '':
                    logging.warning("Received empty message, skipping")
                    if task_queue.use_cloud:
                        task_queue.sqs.delete_message(
                            QueueUrl=task_queue.status_queue_url,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                    continue
                
                # Try to parse as JSON
                try:
                    message_body = json.loads(message_body_raw)
                except json.JSONDecodeError:
                    logging.warning(f"Received malformed JSON status message: {message_body_raw[:100]}... (truncated)")
                    if task_queue.use_cloud:
                        task_queue.sqs.delete_message(
                            QueueUrl=task_queue.status_queue_url,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                    continue
                
                message_type = message_body.get('type', '')
                node_id = message_body.get('node_id', '')
                
                if node_id and message_type == MSG_HEARTBEAT:
                    # Update the active crawler nodes with the heartbeat
                    active_crawler_nodes[node_id] = current_time
                    # Add to metrics if not already there
                    if node_id not in system_metrics["crawler_status"]:
                        system_metrics["crawler_status"][node_id] = "active"
                        system_metrics["crawler_performance"][node_id] = {"assigned": 0, "completed": 0, "failed": 0}
                    
                    logging.debug(f"Received heartbeat from crawler {node_id}")
                
                elif message_type == MSG_URLS_DISCOVERED and 'urls' in message_body:
                    # Process discovered URLs
                    discovered_urls = message_body.get('urls', [])
                    logging.info(f"Received {len(discovered_urls)} URLs from crawler {node_id}")
                    
                    # Add valid URLs to the queue
                    for url in discovered_urls:
                        if is_valid_url(url) and url not in visited_urls:
                            visited_urls.add(url)
                            # Just send the plain URL - simplest approach
                            task_queue.send_message(url)
                            logging.info(f"Added discovered URL to queue: {url}")
                    
                    # Update metrics
                    if node_id in system_metrics["crawler_performance"]:
                        system_metrics["crawler_performance"][node_id]["completed"] += 1
                    system_metrics["urls_crawled"] += 1
                    completed_urls = len(visited_urls) - len(seed_urls)  # Count non-seed URLs as completed
                    processed_urls += 1
                
                elif message_type == MSG_ERROR:
                    # Log error
                    error_msg = message_body.get('error', 'Unknown error')
                    logging.error(f"Error from crawler {node_id}: {error_msg}")
                    system_metrics["error_count"] += 1
                    
                    # If there's a URL in the error message, requeue it
                    failed_url = message_body.get('url', '')
                    if is_valid_url(failed_url) and failed_url not in visited_urls:
                        task_queue.send_message(failed_url)
                        logging.info(f"Requeued failed URL: {failed_url}")
                
                # Delete the message after processing
                if task_queue.use_cloud:
                    task_queue.sqs.delete_message(
                        QueueUrl=task_queue.status_queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                
            except Exception as e:
                logging.error(f"Error processing status message: {e}")
                # Still delete the message to prevent endless retries
                try:
                    if task_queue.use_cloud:
                        task_queue.sqs.delete_message(
                            QueueUrl=task_queue.status_queue_url,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                except Exception:
                    pass
        
        # Check for crawler node timeouts
        for node_id, last_heartbeat in list(active_crawler_nodes.items()):
            if (current_time - last_heartbeat).total_seconds() > heartbeat_timeout_seconds:
                logging.warning(f"Heartbeat timeout for crawler {node_id}. Marking as failed.")
                # Mark node as failed in metrics
                system_metrics["crawler_status"][node_id] = "failed"
                # Remove from active nodes
                del active_crawler_nodes[node_id]
        
        # Update monitoring data
            update_monitoring_data()

        # Get the current queue size
        queue_size = task_queue.get_queue_size()
        
        # Log current status
        logging.info(f"Current status: processed_urls={processed_urls}, queue_size={queue_size}, active_nodes={len(active_crawler_nodes)}")
        
        # Pause briefly before next iteration
        time.sleep(1)
    
    # All URLs processed or max limit reached
    logging.info(f"Processing complete. Processed {processed_urls} URLs.")
    
    # Send shutdown signals to all nodes
    for node_id in system_metrics["crawler_status"]:
        shutdown_message = {
            'type': MSG_TERMINATE,
            'node_id': 'master',
            'target_node': node_id
        }
        task_queue.send_message(json.dumps(shutdown_message))
        logging.info(f"Sent shutdown signal to {node_id}")
    
    # Final metrics update
    system_metrics["end_time"] = datetime.now().isoformat()
    update_monitoring_data()
    
    logging.info("Master node shutting down")
    return

if __name__ == "__main__":
    master_process()