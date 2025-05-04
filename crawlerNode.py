#!/usr/bin/env python3
import os
import time
import logging
import requests
import threading
import random
import socket
import signal
import sys
import json
import urllib.parse
import boto3
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

from cloud_queue   import CloudQueue
from cloud_storage import CloudStorage
from db_manager    import DBManager

# Message types
MSG_URLS_DISCOVERED = 'urls_discovered'
MSG_HEARTBEAT       = 'heartbeat'
MSG_ERROR           = 'error'
MSG_TERMINATE       = 'terminate'

# Global variables for graceful shutdown
shutdown_event = threading.Event()

def normalize_url(base_url, url):
    """
    Normalize a URL:
    - Convert relative URLs to absolute
    - Remove fragments
    - Handle common edge cases
    """
    # Handle empty or None URLs
    if not url:
        return None

    # Convert relative URLs to absolute
    if not url.startswith(('http://', 'https://')):
        url = urljoin(base_url, url)

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

def signal_handler(sig, frame):
    """Handle termination signals gracefully."""
    logging.info("Received termination signal. Shutting down gracefully...")
    shutdown_event.set()

def crawler_process(
    sqs_queue: str = 'crawler-url-queue',
    status_queue: str = 'crawler-status-queue',
    bucket: str = None,
    region: str = None
):
    """
    Crawler node:
      - Sets default AWS region for all boto3 clients
      - Dequeues URLs from SQS
      - Fetches HTML
      - Uploads page HTML to S3 under a URL-derived key
      - Records each fetch in DynamoDB (crawled-urls & node-status tables)
      - Normalizes and extracts URLs
      - Sends discovery & heartbeat status messages back to master
      - Handles graceful shutdown
    """
    # ─── Signal handlers for graceful shutdown ─────────────────────────
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # ─── AWS region setup ─────────────────────────────────────────────
    if region:
        boto3.setup_default_session(region_name=region)

    # ─── Logging setup ───────────────────────────────────────────────
    os.makedirs("logs", exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - Crawler - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("logs/crawler.log"),
            logging.StreamHandler()
        ]
    )
    logging.info("Crawler node started")

    # ─── AWS / DB clients ────────────────────────────────────────────
    task_queue = CloudQueue(queue_name=sqs_queue, status_queue_name=status_queue)
    storage    = CloudStorage(bucket_name=bucket, region=region)
    db         = DBManager()

    node_id  = socket.gethostname()

    # ─── Crawler stats ───────────────────────────────────────────────
    stats = {
        'urls_crawled': 0,
        'urls_discovered': 0,
        'errors': 0,
        'start_time': datetime.utcnow().isoformat()
    }

    # ─── Heartbeat thread ───────────────────────────────────────────
    def heartbeat():
        while not shutdown_event.is_set():
            try:
                # 1) SQS heartbeat with stats
                task_queue.send_status_message({
                    'type': MSG_HEARTBEAT,
                    'node_id': node_id,
                    'timestamp': time.time(),
                    'stats': stats
                })
                # 2) DynamoDB node-status
                db.update_node_status(node_id, {
                    'last_beat': datetime.utcnow().isoformat(),
                    'stats': stats
                })
            except Exception as e:
                logging.error(f"Error in heartbeat thread: {e}")

            # Sleep for 5 seconds or until shutdown
            shutdown_event.wait(5)

    heartbeat_thread = threading.Thread(target=heartbeat, daemon=True)
    heartbeat_thread.start()

    # ─── Main crawl loop ────────────────────────────────────────────
    try:
        while not shutdown_event.is_set():
            try:
                # Check for termination messages first
                status_messages = task_queue.receive_status_messages(WaitTimeSeconds=1, MaxNumber=5)
                for status_msg in status_messages:
                    try:
                        body = json.loads(status_msg.get('Body', '{}'))
                        if body.get('type') == MSG_TERMINATE:
                            logging.info("Received terminate message from master")
                            shutdown_event.set()
                            break
                    except Exception:
                        pass  # Ignore malformed messages
                    finally:
                        task_queue.delete_status_message(status_msg)

                # Exit the loop if shutdown is requested
                if shutdown_event.is_set():
                    break

                # Get a URL to crawl
                messages = task_queue.receive_messages(WaitTimeSeconds=5, MaxNumber=1)
                if not messages:
                    continue

                msg = messages[0]
                try:
                    url_to_crawl = msg.get('Body', '')
                    if not url_to_crawl:
                        url_to_crawl = getattr(msg, 'body', '')
                except (TypeError, KeyError, AttributeError):
                    try:
                        url_to_crawl = msg.body
                    except (AttributeError, TypeError):
                        logging.error(f"Could not extract URL from message: {msg}")
                        task_queue.delete_message(msg)
                        continue

                # Skip empty or invalid URLs
                if not url_to_crawl or not isinstance(url_to_crawl, str):
                    logging.warning(f"Skipping invalid URL: {url_to_crawl}")
                    task_queue.delete_message(msg)
                    continue

                # Delete the message from the queue
                task_queue.delete_message(msg)

                # Normalize the URL before crawling
                url_to_crawl = url_to_crawl.strip()
                if not url_to_crawl.startswith(('http://', 'https://')):
                    url_to_crawl = 'http://' + url_to_crawl

                # Implement politeness - random delay between 1-3 seconds
                time.sleep(random.uniform(1, 3))

                # Fetch the page with proper headers and timeout
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                    'Cache-Control': 'max-age=0'
                }

                resp = requests.get(url_to_crawl, headers=headers, timeout=15, allow_redirects=True)
                resp.raise_for_status()  # Raise exception for 4XX/5XX status codes

                # Check content type - only process HTML
                content_type = resp.headers.get('Content-Type', '')
                if not content_type.startswith('text/html'):
                    logging.info(f"Skipping non-HTML content: {url_to_crawl} (Content-Type: {content_type})")
                    continue

                # ─── Upload HTML to S3 ────────────────────────────
                key = urllib.parse.quote_plus(url_to_crawl)
                ok = storage.upload_content(key, resp.text.encode('utf-8'))
                if ok:
                    logging.info(f"Storage upload OK for key: {key}")
                    # Mark it in DynamoDB
                    db.mark_url_as_fetched(url_to_crawl)
                else:
                    logging.error(f"Storage upload FAILED for key: {key}")

                # ─── Record crawled URL in DynamoDB ────────────────
                db.add_crawled_url(
                    url_to_crawl,
                    metadata={
                        'status_code': resp.status_code,
                        'fetched_at': datetime.now().isoformat(),
                        'content_type': content_type
                    }
                )

                # Update stats
                stats['urls_crawled'] += 1

                # ─── Extract and normalize links ─────────────────────────────────
                soup = BeautifulSoup(resp.text, 'html.parser')
                extracted_raw = []

                # Extract links from <a> tags
                for a in soup.find_all('a', href=True):
                    href = a['href']
                    normalized_url = normalize_url(url_to_crawl, href)
                    if normalized_url:
                        extracted_raw.append(normalized_url)

                # Remove duplicates
                extracted = list(set(extracted_raw))

                # Limit the number of URLs to avoid overwhelming the system
                if len(extracted) > 50:
                    extracted = random.sample(extracted, 50)

                # Update stats
                stats['urls_discovered'] += len(extracted)

                # ─── Send discovered URLs to master ────────────────
                task_queue.send_status_message({
                    'type': MSG_URLS_DISCOVERED,
                    'node_id': node_id,
                    'url': url_to_crawl,
                    'urls': extracted,
                    'timestamp': time.time(),
                    'stats': {
                        'url_count': len(extracted),
                        'content_size': len(resp.text)
                    }
                })
                logging.info(f"Crawled {url_to_crawl}, found {len(extracted)} links")

            except requests.exceptions.RequestException as e:
                if 'url_to_crawl' in locals():
                    logging.error(f"Request error for {url_to_crawl}: {e}")
                    stats['errors'] += 1
                    task_queue.send_status_message({
                        'type': MSG_ERROR,
                        'node_id': node_id,
                        'url': url_to_crawl,
                        'error': f"Request error: {str(e)}"
                    })
            except Exception as e:
                if 'url_to_crawl' in locals():
                    logging.error(f"Error crawling {url_to_crawl}: {e}")
                    stats['errors'] += 1
                    task_queue.send_status_message({
                        'type': MSG_ERROR,
                        'node_id': node_id,
                        'url': url_to_crawl,
                        'error': str(e)
                    })
                else:
                    logging.error(f"Unexpected error in crawler: {e}")

    finally:
        # Perform cleanup
        logging.info("Crawler shutting down...")

        # Send final heartbeat with shutdown notification
        try:
            task_queue.send_status_message({
                'type': MSG_HEARTBEAT,
                'node_id': node_id,
                'timestamp': time.time(),
                'stats': stats,
                'status': 'shutting_down'
            })
        except Exception as e:
            logging.error(f"Error sending final heartbeat: {e}")

        # Wait for heartbeat thread to finish
        heartbeat_thread.join(timeout=2.0)

        # Shutdown cloud services
        try:
            task_queue.shutdown_queue()
        except Exception as e:
            logging.error(f"Error shutting down queue: {e}")

        logging.info("Crawler shutdown complete")

if __name__ == "__main__":
    import sys
    crawler_process(*sys.argv[1:])
