#!/usr/bin/env python3
import sys
import logging
import argparse
from cloud_queue import CloudQueue

def setup_logging():
    """Set up basic logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )

def add_seed_urls(queue):
    """Add seed URLs to the queue"""
    seed_urls = [
        "https://www.python.org",
        "https://www.github.com",
        "https://www.wikipedia.org",
        "https://www.reddit.com",
        "https://www.stackoverflow.com"
    ]
    
    for url in seed_urls:
        logging.info(f"Adding URL: {url}")
        # Send as plain text - this is important, don't use enqueue_url
        queue.send_message(url)
    
    logging.info(f"Added {len(seed_urls)} URLs to the queue")

def show_queue_status(queue):
    """Display the current status of the queue"""
    size = queue.get_queue_size()
    logging.info(f"Current queue size: {size} messages")
    
    # If queue is not empty, try to peek at messages without consuming them
    if size > 0:
        messages = queue.receive_messages(max_messages=5, wait_time=1)
        logging.info(f"Peeked at {len(messages)} messages:")
        for i, message in enumerate(messages):
            body = message.get('Body', '')
            logging.info(f"  {i+1}. {body[:100]}...")
        
        # Don't delete the messages - we just peeked at them

def purge_queue(queue):
    """Purge all messages from the queue"""
    logging.info(f"Purging queue {queue.queue_url}...")
    
    if queue.use_cloud:
        try:
            queue.sqs.purge_queue(QueueUrl=queue.queue_url)
            logging.info("Queue purged!")
        except Exception as e:
            logging.error(f"Error purging queue: {e}")
    else:
        queue.local_queue.clear()
        queue._persist_local_queue()
        logging.info("Local queue purged!")

def main():
    """Main function for queue debugging"""
    parser = argparse.ArgumentParser(description="Debug utility for CloudQueue")
    parser.add_argument("action", choices=["add", "status", "purge"], 
                       help="Action to perform: add (seed URLs), status (check queue), purge (clear queue)")
    parser.add_argument("--queue", default="crawler-url-queue", 
                       help="Queue name to operate on")
    
    args = parser.parse_args()
    
    setup_logging()
    queue = CloudQueue(queue_name=args.queue)
    
    if args.action == "add":
        add_seed_urls(queue)
    elif args.action == "status":
        show_queue_status(queue)
    elif args.action == "purge":
        purge_queue(queue)

if __name__ == "__main__":
    main() 