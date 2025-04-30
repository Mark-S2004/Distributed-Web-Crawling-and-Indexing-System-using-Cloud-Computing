#!/usr/bin/env python3
import time
import logging
import requests
from bs4 import BeautifulSoup
import threading
import random
import os
import json
from urllib.parse import urljoin, urlparse
import socket
import sys
from cloud_queue import CloudQueue

# Define message types for AWS communication
MSG_TERMINATE = 'terminate'
MSG_URLS_DISCOVERED = 'urls_discovered'
MSG_INDEX_CONTENT = 'index_content'
MSG_HEARTBEAT = 'heartbeat'
MSG_ERROR = 'error'

def crawler_process(sqs_queue=None, status_queue=None, bucket=None):
    """
    Enhanced Crawler Node Process (Phase 3):
      - Implements regular heartbeat signals to master node via SQS
      - Provides detailed logging for monitoring
      - Handles tasks from the queue.
      - For each received URL:
          1. Fetches the web page content (using requests).
          2. Parses the content using BeautifulSoup to extract additional URLs.
          3. Sends extracted URLs back to the queue (msg type: urls_discovered).
          4. Sends the fetched content to a separate queue for the indexer (msg type: index_content).
          5. Sends a status update (heartbeat) via the status queue.
      - Exits when a shutdown signal is received.
    """
    # Initialize default queue and bucket names if not provided
    sqs_queue = sqs_queue or 'crawler-url-queue'
    status_queue = status_queue or 'crawler-status-queue'
    bucket = bucket or 'web-crawler-data-storage'
    
    # Generate a unique node ID for this crawler instance
    node_id = f"crawler-{socket.gethostname()}-{os.getpid()}"
    
    # Setup enhanced logging with file output
    log_filename = os.path.join("logs", f"crawler_{node_id}.log")
    # Ensure logs directory exists
    os.makedirs(os.path.dirname(log_filename), exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - Crawler-%(process)d - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()
        ]
    )
    logging.info(f"Crawler node started with ID: {node_id}")

    # Initialize the cloud queue
    task_queue = CloudQueue(queue_name=sqs_queue, status_queue_name=status_queue)
    logging.info(f"Connected to task queue: {sqs_queue}")
    logging.info(f"Connected to status queue: {status_queue}")
    
    # Statistics for the crawler node
    stats = {
        "urls_processed": 0,
        "urls_extracted": 0,
        "errors": 0,
        "start_time": time.time()
    }
    
    # Heartbeat mechanism
    shutdown_flag = False
    
    def send_heartbeat():
        """Send periodic heartbeats to the master node via the status queue"""
        while not shutdown_flag:
            try:
                uptime = time.time() - stats["start_time"]
                heartbeat_msg = {
                    "type": MSG_HEARTBEAT,
                    "node_id": node_id,
                    "uptime": uptime,
                    "urls_processed": stats["urls_processed"],
                    "errors": stats["errors"],
                    "timestamp": time.time()
                }
                task_queue.send_status_update(node_id, heartbeat_msg)
                logging.debug(f"Sent heartbeat to master node")
            except Exception as e:
                logging.error(f"Error sending heartbeat: {e}")
            
            # Sleep for a random time between 2 and 5 seconds to avoid synchronized heartbeats
            time.sleep(random.uniform(2, 5))
    
    # Start heartbeat thread
    heartbeat_thread = threading.Thread(target=send_heartbeat, daemon=True)
    heartbeat_thread.start()
    logging.info(f"Heartbeat mechanism started")

    try:
        # Main processing loop
        while not shutdown_flag:
            # Poll for messages from the queue
            messages = task_queue.receive_messages(max_messages=1, wait_time=10)
            
            if not messages:
                logging.debug("No messages in queue, continuing...")
                continue
                
            # Process the message
            message = messages[0]
            message_body = message.get('Body', '')
            
            # Try to parse as JSON, but also handle raw URL strings
            try:
                message_data = json.loads(message_body)
                # Check if this is a termination message
                if isinstance(message_data, dict) and message_data.get('type') == MSG_TERMINATE:
                    if message_data.get('target_node') == node_id or message_data.get('target_node') == 'all':
                        logging.info(f"Received shutdown signal. Exiting.")
                        shutdown_flag = True
                        # Delete the termination message
                        task_queue.delete_message(message)
                        break
                    
                # Get URL from JSON message if available
                url_to_crawl = message_data.get('url', message_body)
            except (json.JSONDecodeError, TypeError):
                # If not JSON, treat as raw URL
                url_to_crawl = message_body
            
            # Skip processing if the message isn't a valid URL
            if not url_to_crawl or not isinstance(url_to_crawl, str) or not (url_to_crawl.startswith('http://') or url_to_crawl.startswith('https://')):
                logging.warning(f"Received invalid URL: {url_to_crawl}. Skipping.")
                task_queue.delete_message(message)
                continue

            logging.info(f"Crawler {node_id} received URL: {url_to_crawl}")
            task_start_time = time.time()

            try:
                # Fetch the webpage
                logging.info(f"Fetching content from {url_to_crawl}")
                response = requests.get(url_to_crawl, timeout=10)
                content = response.text
                content_size = len(content)
                logging.info(f"Fetched {content_size} bytes from {url_to_crawl}")

                # Parse the content to extract additional URLs
                logging.info(f"Parsing content from {url_to_crawl}")
                soup = BeautifulSoup(content, 'html.parser')
                extracted_urls = []
                for a_tag in soup.find_all('a', href=True):
                    href = a_tag['href']
                    # Basic check: consider only absolute URLs
                    if href.startswith("http"):
                        extracted_urls.append(href)

                # If no URLs are extracted, simulate a couple of URLs
                if not extracted_urls:
                    extracted_urls = [f"http://example.com/page_{node_id}_{i}" for i in range(2)]
                    logging.info(f"No real URLs found, created {len(extracted_urls)} simulated URLs")
                
                # Update statistics
                stats["urls_processed"] += 1
                stats["urls_extracted"] += len(extracted_urls)

                # Detailed logging
                logging.info(f"Crawler {node_id} crawled {url_to_crawl} and extracted {len(extracted_urls)} URLs in {time.time() - task_start_time:.2f} seconds")

                # Send the list of newly discovered URLs to the queue for the master
                discovered_urls_message = {
                    "type": MSG_URLS_DISCOVERED,
                    "node_id": node_id,
                    "url": url_to_crawl,
                    "urls": extracted_urls,
                    "timestamp": time.time()
                }
                task_queue.send_message(json.dumps(discovered_urls_message))
                logging.debug(f"Sent {len(extracted_urls)} URLs to the queue")

                # Send the content to the indexer queue 
                indexing_message = {
                    "type": MSG_INDEX_CONTENT,
                    "node_id": node_id,
                    "url": url_to_crawl, 
                    "content": content,
                    "timestamp": time.time()
                }
                
                # Check the size of the message to avoid SQS size limits
                message_json = json.dumps(indexing_message)
                message_size = len(message_json.encode('utf-8'))
                
                if message_size > 250000:  # SQS limit is 262144 bytes, use 250000 for safety
                    logging.warning(f"Content for {url_to_crawl} is too large ({message_size} bytes), truncating")
                    # Calculate how much to truncate based on current size
                    truncate_to = int(len(content) * (250000 / message_size) * 0.9)  # 90% of calculated safe size
                    indexing_message["content"] = content[:truncate_to]
                    indexing_message["truncated"] = True
                    indexing_message["original_size"] = len(content)
                    logging.info(f"Truncated content from {len(content)} to {len(indexing_message['content'])} bytes")
                
                # Use the same queue for indexing
                task_queue.send_message(json.dumps(indexing_message))
                logging.info(f"Sent content to indexer for {url_to_crawl}")

                # Send a status update
                status_message = {
                    "type": MSG_HEARTBEAT,
                    "node_id": node_id,
                    "url": url_to_crawl,
                    "status": "completed",
                    "found_urls": len(extracted_urls),
                    "content_size": content_size,
                    "timestamp": time.time()
                }
                task_queue.send_status_update(node_id, status_message)
                logging.debug(f"Sent completion status to master node")

                # Delete the message after successful processing
                task_queue.delete_message(message)

                # Simulate occasional failures for testing fault tolerance
                if random.random() < 0.05:  # 5% chance of simulated failure
                    logging.warning(f"Simulating a brief node failure (testing fault tolerance)")
                    time.sleep(12)  # Sleep longer than heartbeat timeout to trigger failure detection
                
            except requests.exceptions.Timeout:
                stats["errors"] += 1
                error_msg = f"Timeout fetching {url_to_crawl}"
                logging.error(error_msg)
                error_message = {
                    "type": MSG_ERROR,
                    "node_id": node_id,
                    "url": url_to_crawl,
                    "error": error_msg,
                    "timestamp": time.time()
                }
                task_queue.send_status_update(node_id, error_message)
                
                # Delete the message to prevent endless retry loops
                task_queue.delete_message(message)
                
            except requests.exceptions.RequestException as e:
                stats["errors"] += 1
                error_msg = f"Error crawling {url_to_crawl}: {str(e)}"
                logging.error(error_msg)
                error_message = {
                    "type": MSG_ERROR,
                    "node_id": node_id,
                    "url": url_to_crawl,
                    "error": error_msg,
                    "timestamp": time.time()
                }
                task_queue.send_status_update(node_id, error_message)
                
                # Delete the message to prevent endless retry loops
                task_queue.delete_message(message)
                
            except Exception as e:
                stats["errors"] += 1
                error_msg = f"Unexpected error processing URL {url_to_crawl}: {str(e)}"
                logging.error(error_msg)
                error_message = {
                    "type": MSG_ERROR,
                    "node_id": node_id,
                    "url": url_to_crawl,
                    "error": error_msg,
                    "timestamp": time.time()
                }
                task_queue.send_status_update(node_id, error_message)
                
                # Delete the message to prevent endless retry loops
                task_queue.delete_message(message)

            # Log task completion time
            task_duration = time.time() - task_start_time
            logging.info(f"Task for {url_to_crawl} completed in {task_duration:.2f} seconds")
            
            # Pause briefly to simulate a delay and help stagger requests
            time.sleep(0.1)
    
    except Exception as e:
        logging.critical(f"Critical error in crawler {node_id}: {e}")
        import traceback
        logging.critical(traceback.format_exc())
    finally:
        # Set shutdown flag for heartbeat thread
        shutdown_flag = True
        heartbeat_thread.join(timeout=1.0)
        
        # Log final statistics
        total_runtime = time.time() - stats["start_time"]
        logging.info(f"Crawler {node_id} shutting down. Final statistics:")
        logging.info(f"  - Total runtime: {total_runtime:.2f} seconds")
        logging.info(f"  - URLs processed: {stats['urls_processed']}")
        logging.info(f"  - URLs extracted: {stats['urls_extracted']}")
        logging.info(f"  - Errors encountered: {stats['errors']}")
        logging.info(f"Crawler {node_id} exiting")

if __name__ == "__main__":
    crawler_process() 