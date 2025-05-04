#!/usr/bin/env python3
# masterNode.py

import os
import sys
import time
import json
import logging
import signal
import threading
import socket
import urllib.parse
from datetime import datetime
from urllib.parse import urlparse

from cloud_queue import CloudQueue
from db_manager import DBManager

# Message types
MSG_TERMINATE = 'terminate'
MSG_URLS_DISCOVERED = 'urls_discovered'
MSG_HEARTBEAT = 'heartbeat'
MSG_ERROR = 'error'

# Global variables for graceful shutdown
shutdown_event = threading.Event()

def signal_handler(sig, frame):
    """Handle termination signals gracefully."""
    logging.info("Received termination signal. Shutting down master gracefully...")
    shutdown_event.set()

def normalize_url(url):
    """
    Normalize a URL:
    - Remove fragments
    - Handle common edge cases
    - Ensure consistent format
    """
    # Handle empty or None URLs
    if not url:
        return None

    # Parse the URL
    parsed = urlparse(url)

    # Skip non-HTTP/HTTPS URLs
    if parsed.scheme not in ('http', 'https'):
        return None

    # Remove fragments
    url = parsed.scheme + '://' + parsed.netloc + parsed.path

    # Add query parameters if they exist
    if parsed.query:
        url += '?' + parsed.query

    # Remove trailing slash for consistency
    if url.endswith('/'):
        url = url[:-1]

    return url

def master_process(
    sqs_queue: str = 'crawler-url-queue',
    status_queue: str = 'crawler-status-queue',
    bucket: str = None,
    region: str = None
):
    """
    Master node orchestration:
      • On startup, if url_tracking table is empty, seed a list of URLs
        (writes into DynamoDB and SQS).
      • Listens for crawler heartbeats, discoveries, and errors.
      • Re-queues newly discovered URLs (and tracks them in DynamoDB).
      • Tracks crawler node timeouts based on heartbeats.
      • Handles graceful shutdown.
    """
    # ─── Signal handlers for graceful shutdown ─────────────────────────
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # ─── Logging setup ───────────────────────────────────────────────
    os.makedirs("logs", exist_ok=True)
    # Clear any existing handlers to avoid duplicate logging
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - Master - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("logs/master.log"),
            logging.StreamHandler()
        ]
    )
    logging.info("Master node starting up")

    # ─── AWS clients & DB ───────────────────────────────────────────
    task_queue = CloudQueue(queue_name=sqs_queue, status_queue_name=status_queue)
    db = DBManager()

    node_id = f"master-{socket.gethostname()}"

    # ─── System stats ───────────────────────────────────────────────
    stats = {
        'urls_queued': 0,
        'urls_discovered': 0,
        'errors': 0,
        'active_crawlers': 0,
        'start_time': datetime.now().isoformat()
    }

    # ─── Persistent visited-URLs state ─────────────────────────────
    visited_file = "visited_urls.json"
    if os.path.exists(visited_file):
        try:
            with open(visited_file) as f:
                visited = set(json.load(f))
                logging.info(f"Loaded {len(visited)} visited URLs from {visited_file}")
        except Exception as e:
            logging.warning(f"Could not load {visited_file}: {e}")
            visited = set()
    else:
        visited = set()

    # Create a more efficient data structure for URL deduplication
    url_bloom_filter = set()  # Simple set for now, could be replaced with a proper bloom filter
    for url in visited:
        url_bloom_filter.add(url)

    def save_visited():
        """Save visited URLs to disk periodically."""
        try:
            with open(visited_file, "w") as f:
                json.dump(list(visited), f)
            logging.info(f"Saved {len(visited)} visited URLs to {visited_file}")
        except Exception as e:
            logging.error(f"Error saving visited URLs: {e}")

    # ─── Periodic save thread ───────────────────────────────────────
    def periodic_save():
        """Periodically save visited URLs to disk."""
        while not shutdown_event.is_set():
            try:
                save_visited()
            except Exception as e:
                logging.error(f"Error in periodic save: {e}")

            # Sleep for 5 minutes or until shutdown
            shutdown_event.wait(300)  # 5 minutes

    save_thread = threading.Thread(target=periodic_save, daemon=True)
    save_thread.start()

    # ─── Seed logic (only if url_tracking table is empty) ──────────
    seed_urls = [
        "https://www.python.org",
        "https://en.wikipedia.org/wiki/Python_(programming_language)",
        "https://github.com/python",
        "https://stackoverflow.com/questions/tagged/python",
        "https://docs.python.org/3/",
        "https://pypi.org/",
        "https://www.djangoproject.com/",
        "https://flask.palletsprojects.com/",
        "https://aws.amazon.com/",
        "https://cloud.google.com/",
        "https://azure.microsoft.com/",
        "https://www.reddit.com/r/Python/",
        "https://news.ycombinator.com/",
        "https://www.bbc.com/",
        "https://www.nytimes.com/"
    ]

    # Check if url_tracking has any items
    try:
        resp = db.url_tracking_table.scan(Limit=1)
        if not resp.get('Items'):
            logging.info("URL tracking table is empty. Seeding with initial URLs...")
            for url in seed_urls:
                normalized_url = normalize_url(url)
                if normalized_url:
                    db.add_url_to_tracking(normalized_url)
                    task_queue.send_message(normalized_url)
                    visited.add(normalized_url)
                    url_bloom_filter.add(normalized_url)
                    stats['urls_queued'] += 1
                    logging.info(f"Seeded URL → {normalized_url}")
            save_visited()
    except Exception as e:
        logging.error(f"Error checking/seeding URL tracking table: {e}")

    # ─── State for crawler heartbeats ────────────────────────────────
    active_heartbeats = {}        # node_id → last heartbeat time
    heartbeat_timeout = 60        # seconds (increased from 30)
    crawler_stats = {}            # node_id → stats dict

    # ─── Main loop ───────────────────────────────────────────────────
    try:
        while not shutdown_event.is_set():
            try:
                # Fetch status messages from crawlers
                msgs = task_queue.receive_status_messages(WaitTimeSeconds=5, MaxNumber=10)
                for m in msgs:
                    try:
                        body = json.loads(m.get('Body', '{}'))
                        mtype = body.get('type')
                        node = body.get('node_id', 'unknown')

                        if mtype == MSG_HEARTBEAT:
                            # Update heartbeat timestamp
                            active_heartbeats[node] = datetime.now()

                            # Update crawler stats if available
                            if 'stats' in body:
                                crawler_stats[node] = body['stats']

                            # Update active crawler count
                            stats['active_crawlers'] = len(active_heartbeats)

                        elif mtype == MSG_URLS_DISCOVERED:
                            # Process discovered URLs
                            source_url = body.get('url', '')
                            new_urls = body.get('urls', [])

                            # Update stats
                            stats['urls_discovered'] += len(new_urls)

                            # Process each URL
                            queued_count = 0
                            for u in new_urls:
                                # Normalize the URL
                                normalized_url = normalize_url(u)
                                if not normalized_url:
                                    continue

                                # Check if we've seen this URL before (fast check)
                                if normalized_url in url_bloom_filter:
                                    continue

                                # Double-check in the full set (slower but definitive)
                                if normalized_url not in visited:
                                    # Add to tracking and queue
                                    db.add_url_to_tracking(normalized_url)
                                    task_queue.send_message(normalized_url)

                                    # Mark as visited
                                    visited.add(normalized_url)
                                    url_bloom_filter.add(normalized_url)

                                    # Update stats
                                    stats['urls_queued'] += 1
                                    queued_count += 1

                            if queued_count > 0:
                                logging.info(f"Queued {queued_count} new URLs from {source_url}")

                        elif mtype == MSG_ERROR:
                            # Log crawler errors
                            url = body.get('url', 'unknown')
                            error = body.get('error', 'unknown error')
                            logging.error(f"[{node}] Error crawling {url}: {error}")
                            stats['errors'] += 1

                        elif mtype == MSG_TERMINATE:
                            logging.info("Terminate signal received. Shutting down master.")
                            shutdown_event.set()
                            break

                    except Exception as e:
                        logging.error(f"Error processing message: {e}")
                    finally:
                        # Always delete the processed message
                        task_queue.delete_status_message(m)

                # Detect timed-out crawlers
                now = datetime.now()
                for node_id, ts in list(active_heartbeats.items()):
                    if (now - ts).total_seconds() > heartbeat_timeout:
                        logging.warning(f"Crawler {node_id} timed out")
                        del active_heartbeats[node_id]
                        if node_id in crawler_stats:
                            del crawler_stats[node_id]

                # Update active crawler count
                stats['active_crawlers'] = len(active_heartbeats)

                # Log periodic status
                if int(time.time()) % 60 == 0:  # Log every minute
                    logging.info(f"Status: {len(active_heartbeats)} active crawlers, "
                                f"{stats['urls_queued']} URLs queued, "
                                f"{stats['urls_discovered']} URLs discovered, "
                                f"{stats['errors']} errors")

                # Check for shutdown
                if shutdown_event.is_set():
                    break

                # Small delay
                time.sleep(1)

            except Exception as e:
                logging.error(f"Error in master loop: {e}")
                time.sleep(5)  # Wait before retrying

    except Exception as e:
        logging.error(f"Critical master loop exception: {e}")
    finally:
        # Perform cleanup
        logging.info("Master shutting down...")

        # Save visited URLs
        save_visited()

        # Send terminate signal to all crawlers and indexers
        try:
            logging.info("Sending terminate signal to all nodes...")
            terminate_msg = {
                'type': MSG_TERMINATE,
                'node_id': node_id,
                'timestamp': time.time()
            }
            task_queue.send_status_message(terminate_msg)
        except Exception as e:
            logging.error(f"Error sending terminate signal: {e}")

        # Wait for save thread to finish
        save_thread.join(timeout=2.0)

        # Shutdown cloud services
        try:
            task_queue.shutdown_queue()
        except Exception as e:
            logging.error(f"Error shutting down queue: {e}")

        logging.info("Master shutdown complete")

if __name__ == "__main__":
    master_process(*sys.argv[1:])
