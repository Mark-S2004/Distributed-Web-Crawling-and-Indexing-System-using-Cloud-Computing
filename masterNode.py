#!/usr/bin/env python3
import time
import logging
import json
import os
from datetime import datetime, timedelta
from cloud_queue import CloudQueue
from cloud_storage import CloudStorage
from db_manager import DatabaseManager

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

    # Initialize the cloud queue, storage and database
    task_queue = CloudQueue(queue_name=sqs_queue, status_queue_name=status_queue)
    cloud_storage = CloudStorage(bucket_name=bucket)
    db_manager = DatabaseManager()
    logging.info(f"Task queue initialized in {'cloud' if task_queue.is_cloud_mode() else 'local'} mode")
    logging.info(f"Cloud storage initialized with bucket: {bucket}")

    # Function to validate URL before sending to queue
    def is_valid_url(url):
        if not url or not isinstance(url, str):
            return False
        # Basic validation - ensure URL has a scheme
        return url.startswith('http://') or url.startswith('https://')

    # Real seed URLs for testing
    seed_urls = [
        "https://www.python.org",
        "https://www.github.com",
        "https://www.wikipedia.org",
        "https://www.reddit.com",
        "https://www.stackoverflow.com"
    ]
    
    # Load previously visited URLs to prevent reprocessing
    visited_urls_file = os.path.join("data", "visited_urls.json")
    visited_urls = set()
    os.makedirs(os.path.dirname(visited_urls_file), exist_ok=True)
    
    try:
        if os.path.exists(visited_urls_file):
            with open(visited_urls_file, 'r') as f:
                visited_urls = set(json.load(f))
                logging.info(f"Loaded {len(visited_urls)} previously visited URLs")
    except Exception as e:
        logging.error(f"Error loading visited URLs file: {e}")
    
    # Function to save visited URLs
    def save_visited_urls():
        try:
            with open(visited_urls_file, 'w') as f:
                json.dump(list(visited_urls), f)
            logging.info(f"Saved {len(visited_urls)} visited URLs to disk")
        except Exception as e:
            logging.error(f"Error saving visited URLs file: {e}")
    
    # Check queue size first to see if we need to add seed URLs
    queue_size = task_queue.get_queue_size()
    
    # Check if we should purge the queue to avoid reprocessing
    need_queue_reset = False
    last_reset_check = datetime.now() - timedelta(minutes=10)  # Start from a time in the past
    
    # Keep track of active crawler nodes - will be updated based on heartbeats
    active_crawler_nodes = {}  # Format: {node_id: last_heartbeat_time}
    max_urls_to_process = 100  # Increase from 50 to 100 URLs to process
    min_runtime_seconds = 600   # Increase minimum runtime to 10 minutes

    # Initialize processed_urls variable here before it's used
    processed_urls = 0
    
    # Run for at least min_runtime_seconds regardless of queue size, to allow crawler nodes to connect
    start_time = datetime.now()
    min_run_time = timedelta(seconds=min_runtime_seconds)
    
    # Add these seed URLs if the queue is empty and we haven't processed much
    additional_test_urls = [
        "https://en.wikipedia.org/wiki/Python_(programming_language)",
        "https://github.com/python",
        "https://stackoverflow.com/questions/tagged/python",
        "https://docs.python.org/3/",
        "https://pypi.org/",
        "https://www.djangoproject.com/",
        "https://flask.palletsprojects.com/",
        "https://www.bbc.com/",
        "https://news.ycombinator.com/",
        "https://aws.amazon.com/" 
    ]
    
    # If queue is empty but we have active nodes, add test URLs periodically
    if queue_size == 0 and processed_urls < 20 and (datetime.now() - start_time).total_seconds() > 60:
        # Add more test URLs if we've been running for a while but not processing much
        test_urls_added = 0
        for url in additional_test_urls:
            if url not in visited_urls:
                # Add URL to tracking database
                db_manager.add_url_to_tracking(url)
                
                # Add URL to queue
                result = task_queue.send_message(url)
                visited_urls.add(url)
                logging.info(f"Added additional test URL to queue and tracking: {url}")
                test_urls_added += 1
                
        if test_urls_added > 0:
            logging.info(f"Added {test_urls_added} additional test URLs to the queue")
            save_visited_urls()
    
    # Check for reprocessing issues
    if os.path.exists(visited_urls_file) and queue_size > 0 and (datetime.now() - last_reset_check).total_seconds() > 60:
        # Only check periodically (every 60 seconds) to avoid excessive API calls
        last_reset_check = datetime.now()
        logging.info("Checking for URL reprocessing issues...")
        
        # If we already have visited URLs but the queue has items, we might be in a reprocessing loop
        try:
            # Sample some messages from the queue to check if they're already visited
            sample_messages = task_queue.receive_messages(max_messages=5, wait_time=1)
            reprocessed_count = 0
            
            for message in sample_messages:
                message_body = message.get('Body', '')
                
                # Skip if not a URL
                if not is_valid_url(message_body):
                    continue
                
                if message_body in visited_urls:
                    reprocessed_count += 1
                    logging.warning(f"Found already visited URL in queue: {message_body}")
                
                # Put back the message for now
                task_queue.delete_message(message)
            
            # If most sampled messages are reprocessed URLs, clear the queue
            if reprocessed_count >= 3 and len(sample_messages) >= 4:
                logging.warning("Detected URL reprocessing loop! Purging queue...")
                need_queue_reset = True
                
                if task_queue.purge_queue():
                    logging.info("Successfully purged queue to prevent reprocessing")
                    queue_size = 0
                else:
                    logging.error("Failed to purge queue")
        except Exception as e:
            logging.error(f"Error checking for reprocessing: {e}")
    
    if queue_size == 0 or need_queue_reset:
        # Queue is empty or was reset, add seed URLs if not already visited
        for url in seed_urls:
            if url not in visited_urls:
                # Add URL to tracking database
                db_manager.add_url_to_tracking(url)
                
                # Add URL to queue
                result = task_queue.send_message(url)
                visited_urls.add(url)
                logging.info(f"Added seed URL to queue and tracking: {url}, result: {result}")
            else:
                logging.info(f"Seed URL already visited, skipping: {url}")
        
        # Save visited URLs to disk
        save_visited_urls()
        logging.info(f"Added seed URLs to the task queue")
    else:
        logging.info(f"Queue already contains {queue_size} messages, skipping seed URLs")

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
    
    logging.info(f"Using URL tracking with {len(visited_urls)} previously visited URLs")
    if len(visited_urls) > 0:
        logging.info(f"Sample URLs: {list(visited_urls)[:3]}")

    # Main processing loop
    while True:
        try:
            # Check if we've exceeded the maximum URLs to process
            if processed_urls >= max_urls_to_process:
                logging.info(f"Reached maximum URLs to process ({max_urls_to_process})")
                break

            # Check if we've exceeded the minimum runtime
            if (datetime.now() - start_time) < min_run_time:
                logging.info("Still within minimum runtime period")
            else:
                # Check if we should continue based on queue size and active nodes
                if queue_size == 0 and len(active_crawler_nodes) == 0:
                    logging.info("Queue is empty and no active crawler nodes, exiting")
                    break

            # Process status messages from crawler nodes
            status_messages = task_queue.receive_status_messages()
            for message in status_messages:
                try:
                    message_body = json.loads(message.get('Body', '{}'))
                    message_type = message_body.get('type')
                    node_id = message_body.get('node_id')
                    
                    if message_type == MSG_HEARTBEAT:
                        # Update node heartbeat
                        active_crawler_nodes[node_id] = datetime.now()
                        logging.debug(f"Received heartbeat from node {node_id}")
                        
                    elif message_type == MSG_URLS_DISCOVERED:
                        # Process newly discovered URLs
                        new_urls = message_body.get('urls', [])
                        for url in new_urls:
                            if url not in visited_urls:
                                # Add URL to tracking database
                                db_manager.add_url_to_tracking(url)
                                
                                # Add URL to queue
                                task_queue.send_message(url)
                                visited_urls.add(url)
                                logging.info(f"Added discovered URL to queue and tracking: {url}")
                        
                        # Update metrics
                        system_metrics['urls_crawled'] += 1
                        update_monitoring_data()
                        
                    elif message_type == MSG_ERROR:
                        # Handle error message
                        error_msg = message_body.get('error', 'Unknown error')
                        logging.error(f"Error from node {node_id}: {error_msg}")
                        system_metrics['error_count'] += 1
                        update_monitoring_data()
                        
                except Exception as e:
                    logging.error(f"Error processing status message: {e}")
                
                # Delete the message after processing
                task_queue.delete_message(message)

            # Check for inactive nodes and re-queue their tasks
            current_time = datetime.now()
            inactive_nodes = []
            for node_id, last_heartbeat in active_crawler_nodes.items():
                if (current_time - last_heartbeat).total_seconds() > heartbeat_timeout_seconds:
                    inactive_nodes.append(node_id)
                    logging.warning(f"Node {node_id} appears to be inactive")
            
            # Remove inactive nodes
            for node_id in inactive_nodes:
                del active_crawler_nodes[node_id]
            
            # Update metrics
            system_metrics['crawler_status'] = {
                'active_nodes': len(active_crawler_nodes),
                'inactive_nodes': len(inactive_nodes)
            }
            update_monitoring_data()
            
            # Sleep briefly to avoid excessive CPU usage
            time.sleep(1)
            
        except Exception as e:
            logging.error(f"Error in main processing loop: {e}")
            time.sleep(5)  # Sleep longer on error

    # Send termination message to all nodes
    logging.info("Sending termination message to all nodes")
    task_queue.send_status_message({
        'type': MSG_TERMINATE,
        'message': 'Master node is shutting down'
    })
    
    # Save final state
    save_visited_urls()
    logging.info("Master node shutdown complete")

if __name__ == "__main__":
    master_process()